import asyncio
import logging
from functools import reduce
from LocalExecutor import LocalExecutor

logger = logging.getLogger(__name__)

class ParallelTask:
    PENDING = 1
    WORKING = 2
    FINISHED = 3

    def __init__(self, uuid, name, deps, actions, slots_required):
        self.uuid = uuid
        self.status = ParallelTask.PENDING
        self.name = name
        self.deps = deps
        self.actions = actions
        self.successors = []
        self.slots_required = slots_required
        
        self.running_core = None

    def __repr__(self):
        return f"Task #{self.uuid}"

    def add_successor(self, successor_task):
        self.successors.append(successor_task)

class ParallelRunner:
    def __init__(self, uuid, task_executor, notify_ev, dispatch_hint, executor_inst, num_slots):
        self.uuid = uuid
        self.task_executor = task_executor

        # Wake up on two conditions: 
        # 1. new task arrive
        # 2. close event occur
        self.notification_event = notify_ev
        self.dispatch_hint = dispatch_hint
        self.executor_inst = executor_inst
        self.num_slots = num_slots

        self.used_slots = 0
        self.task_insts = {}
        self.async_task_insts = {}

        self.closing = False

        self.available_cores = set([i for i in range(0, self.num_slots)])
        print(f"available_cores: {self.available_cores}")

    def __repr__(self):
        return f"Runner #{self.uuid}"

    def is_available(self):
        """Whether this runner have spare slots left"""
        return self.used_slots < self.num_slots

    def available_slots(self):
        """Return the slots available"""
        return self.num_slots - self.used_slots

    async def run_task(self, task_inst):
        """
        task_inst: ParallelTask
        """
        core = None
        if self.dispatch_hint['bind_core']:
            for possible_guess in self.dispatch_hint['affinity_priority']:
                if possible_guess in self.available_cores:
                    self.available_cores.remove(possible_guess)
                    core = possible_guess
                    break
        assert(core is not None)

        for action in task_inst.actions:
            if self.dispatch_hint['bind_core']:
                # cmdExecuted = f"taskset -c {core} bash -c '{action}'"
                # retcode, stdout = await self.executor_inst.execute(cmdExecuted)
                cmdExecuted = f"{action}"
                retcode, stdout = await self.executor_inst.execute(cmdExecuted)
                self.available_cores.add(core)
            else:
                cmdExecuted = action
                retcode, stdout = await self.executor_inst.execute(cmdExecuted)

            logger.debug(f"[{cmdExecuted}], retcode={retcode}, output={stdout}")

    async def wait_for_event_helper(self):
        await self.notification_event.wait()
        # logger.debug(f"Runner {self.uuid} have waited the lock.")

    async def run(self):
        logger.debug(f"Runner {self.uuid} have started.")

        # coroutine
        event_wait_task = asyncio.create_task(
            self.wait_for_event_helper(),
            name=f"Runner_{self.uuid}_whelper"
        )
        awaitables = {
            event_wait_task
        }
        while len(awaitables):
            done, pending = await asyncio.wait(awaitables, return_when=asyncio.FIRST_COMPLETED)
            # logger.debug(f"Runner {self.uuid} done={done}")
            if event_wait_task in done:
                # logger.debug(f"Runner {self.uuid} got event_wait_task")
                if not self.task_executor.unallocated_tasks_available:
                    # Message for leaving!
                    self.closing = True
                    if len(self.task_insts) == 0:
                        logger.debug(f"Runner {self.uuid} have completed. Exiting")
                        break
                else:
                    task = self.task_executor.get_task(self)
                    self.task_insts[task.uuid] = task
                    async_task = asyncio.create_task(
                        self.run_task(task),
                        name=f"Runner_{self.uuid}_task_{task.uuid}"
                    )
                    self.async_task_insts[async_task] = task.uuid

                    self.used_slots += task.slots_required
                    awaitables.add(async_task)

                self.notification_event.clear()
                done.remove(event_wait_task)
                awaitables.remove(event_wait_task)

                # in closing mode we no longer monitor the event
                # instead we only consider the current ones
                if not self.closing:
                    # add a new one instead
                    event_wait_task = asyncio.create_task(
                        self.wait_for_event_helper(),
                        name=f"Runner_{self.uuid}_whelper"
                    )
                    awaitables.add(event_wait_task)
            
            for async_task in done:
                # logger.debug(f"Processing async task {async_task}")
                task_uuid = self.async_task_insts[async_task]
                self.used_slots -= self.task_insts[task_uuid].slots_required
                self.task_executor.mark_complete(self.uuid, task_uuid)
                del self.task_insts[task_uuid]
                del self.async_task_insts[async_task]
                awaitables.remove(async_task)
            
            self.task_executor.update_alloc()




class ParallelTaskExecutor:
    RUNNER_IDLE = 0
    RUNNER_WORKING = 1  # working, but have slots left
    RUNNER_BUSY = 2

    def __init__(self):
        self.task_insts = {}
        self.registered_runners = {}

        # tasks to be given to the runners
        self.runner_allocated_task = {}

        # asyncio.Events() for each runner
        # set if they've got works to do
        self.runner_notifications = {}

        self.next_task_uuid = 1000
        self.next_runner_uuid = 0


        # key: task.uuid value: task in task_insts
        self.pending_tasks = {}
        self.working_tasks = {}
        self.finished_tasks = {}

        self.unallocated_tasks_available = False

        self.async_runner_tasks = []

    def alloc_task_uuid(self):
        ret = self.next_task_uuid
        self.next_task_uuid += 1

        return ret

    def alloc_runner_uuid(self):
        ret = self.next_runner_uuid
        self.next_runner_uuid += 1

        return ret

    def get_task_by_uuid(self, uuid):
        return self.task_insts[uuid]
    
    def add_task(self, name, deps, actions, slots_required):
        """Construct a parallel task.
        name: name of the task
        deps: List[UUID]
        actions: List[Coroutine], actions to complete this goal

        **Need to call update_alloc() manually after adding all tasks & runners**

        returns the task uuid created.
        """
        new_task = ParallelTask(self.alloc_task_uuid(), name, deps, actions, slots_required)
        for dep_task in deps:
            self.task_insts[dep_task].add_successor(new_task)

        self.task_insts[new_task.uuid] = new_task
        self.pending_tasks[new_task.uuid] = new_task

        self.unallocated_tasks_available = True
        
        logger.debug(f"Added Task {new_task.uuid}: deps={deps}, actions={actions}, slots_required={slots_required}")
        return new_task.uuid

        # See if we have spare runners

    def add_runner(self, dispatch_hint, executor_inst, num_slots):
        """
        **Need to call update_alloc() manually after adding all tasks & runners**
        """
        ev = asyncio.Event()
        runner = ParallelRunner(
            self.alloc_runner_uuid(),
            self,
            ev,
            dispatch_hint,
            executor_inst,
            num_slots
        )
        self.registered_runners[runner.uuid] = runner
        self.runner_notifications[runner.uuid] = ev

    def start_runners(self):
        for runner in self.registered_runners.values():
            self.async_runner_tasks.append(
                asyncio.create_task(
                    runner.run(),
                    name=f"Runner_{runner.uuid}"
                )
            )
        
    async def wait_until_finish(self):
        await asyncio.gather(*self.async_runner_tasks)

    def mark_complete(self, runner_uuid, task_uuid):
        """Mark runner completed working on task"""
        self.task_insts[task_uuid].status = ParallelTask.FINISHED
        logger.debug(f"mark_complete: Runner #{runner_uuid} reported the completion of {task_uuid}")

        self.finished_tasks[task_uuid] = self.working_tasks[task_uuid]
        del self.working_tasks[task_uuid]

    def update_alloc(self):
        """Hook to give latest allocations"""
        # only consider runners with remaining slots
        available_runners = [runner for runner in self.registered_runners.values()
                                    if runner.is_available()]
        
        # logger.debug(f"available_runners={available_runners}")
        for runner in available_runners:
            # atmost one task waiting to be taken for one runner
            if runner.uuid in self.runner_allocated_task:
                continue
            
            # Find possible allocation plan
            available_slots = runner.available_slots()
            candidate_tasks = [task for task in self.pending_tasks.values()
                                    if task.slots_required <= available_slots]
            # print(f"{candidate_tasks}")
            candidate_tasks = [task for task in candidate_tasks
                                    if task not in self.runner_allocated_task.values()]
            # print(f"{candidate_tasks}")
            # Find tasks whose dependencies have been met
            candidate_tasks = [task for task in candidate_tasks
                                    if reduce(
                                        lambda m, n: m+n,
                                        map(
                                            lambda x: (1 if self.task_insts[x].status < ParallelTask.FINISHED else 0),
                                            task.deps
                                        ),
                                        0
                                    ) == 0]
            # print(f"{candidate_tasks}")

            # TODO: sort out the most suitable using the dispatch hint
            if len(candidate_tasks) > 0:
                logger.debug(f"Assigned task {candidate_tasks[0].uuid} to {runner.uuid}")
                self.runner_allocated_task[runner.uuid] = candidate_tasks[0]
                self.runner_notifications[runner.uuid].set()
        
        self.unallocated_tasks_available = False
        for task in self.pending_tasks.values():
            if task.uuid not in self.runner_allocated_task:
                self.unallocated_tasks_available = True
                break

        if not self.unallocated_tasks_available:
            # notify others
            for ev in self.runner_notifications.values():
                ev.set()
        
        # logger.debug(f"unallocated_tasks_available={self.unallocated_tasks_available} {self.runner_notifications}")

    def get_task(self, runner):
        """Get task on behalf of runner
        only call this when you are called
        Notice: this function shall not update_alloc
        """

        assert(runner.uuid in self.runner_allocated_task)
        task = self.runner_allocated_task[runner.uuid]

        del self.runner_allocated_task[runner.uuid]
        self.runner_notifications[runner.uuid].clear()

        task.status = ParallelTask.WORKING
        self.working_tasks[task.uuid] = self.pending_tasks[task.uuid]
        del self.pending_tasks[task.uuid]

        return task

async def test():
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG
    )

    executors = [LocalExecutor() for i in range(0,1)]
    task_executor = ParallelTaskExecutor()
    for executor in executors:
        # last one the highest priority
        task_executor.add_runner({"affinity_priority": [0,5,1,4,2,3], "bind_core": True}, executor, 6)
    
    task_a = task_executor.add_task("task_a", [], ['echo -n "A" && sleep 1'], 1)
    task_b = task_executor.add_task("task_b", [], ['echo -n "B" && sleep 1'], 1)
    task_c = task_executor.add_task("task_c", [task_a], ['echo -n "C"'], 1)
    task_d = task_executor.add_task("task_d", [task_b], ['echo -n "D"'], 1)

    task_executor.update_alloc()
    task_executor.start_runners()

    # await asyncio.sleep(1)
    await task_executor.wait_until_finish()

if __name__ == '__main__':
    asyncio.run(test())