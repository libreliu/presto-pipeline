"""Multiplexed async remote executor

"""

import asyncio, struct, time, sys
import functools
import logging

logger = logging.getLogger(__name__)

PING = 1
EXECUTE = 2
EXECUTE_NO_OUTPUT = 3

PONG = 100
EXECUTE_RESP = 101
EXECUTE_NO_OUTPUT_RESP = 103

class ExecutorClient:
    """Usage:
    client = ExecutorClient('some_host')
    await client.connect()
    
    """
    def __init__(self, host, port=11451):
        self._host = host
        self._port = port
        self._client = None
        self._reader = None
        self._writer = None
        self._next_seq = 0
        # Prevent from multiple handlers writing simutaenously
        # - (In theory no await => no reschedule, but who knows?)
        self._write_lock = None
        self._waiting_msg = None

    async def connect(self):
        self._client = asyncio.open_connection(
            self._host, self._port)
        self._reader, self._writer = await self._client
        self._write_lock = asyncio.Lock()
        self._waiting_msg = dict()
        self._incoming_msg = dict()

        asyncio.create_task(self._queueHandler())

    async def _queueHandler(self):
        try:
            while True:
                header = await self._reader.readexactly(10)
                msgSize, msgType, msgID = struct.unpack('!IHI', header)
                assert(msgID not in self._incoming_msg.keys())
                self._incoming_msg[msgID] = header + await self._reader.readexactly(msgSize - 10)
                self._waiting_msg[msgID].set()

        except asyncio.exceptions.IncompleteReadError as e:
            logger.info(f"Connection to server closed.")
            # TODO: escape

    async def _wait_message(self, expectedID):
        assert(expectedID not in self._waiting_msg)
        self._waiting_msg[expectedID] = asyncio.Event()
        await self._waiting_msg[expectedID].wait()

        msg = self._incoming_msg[expectedID]

        #self._waiting_msg[expectedID].clear()
        del self._waiting_msg[expectedID]
        del self._incoming_msg[expectedID]

        return msg

    async def ping(self):
        """Returns time spent in seconds, using float"""
        assert(self._client is not None)
        start_time = time.perf_counter()

        # -- naturally ensures atomicity --
        seq_num = self._next_seq
        self._next_seq += 1
        # ---------------------------------

        async with self._write_lock:
            self._writer.write(
                struct.pack('!IHI', 4 + 2 + 4, PING, seq_num)
            )
            await self._writer.drain()

        message = await self._wait_message(seq_num)
        msgSize, msgType, msgID = struct.unpack('!IHI', message)
        assert(msgType == PONG and msgID == seq_num)

        end_time = time.perf_counter()
        return end_time - start_time

    async def execute(self, cmd):
        logger.info(f"EXECUTE: {cmd}")

        # TODO: check proper encoding
        cmd = cmd.encode('utf-8')

        # -- naturally ensures atomicity --
        seq_num = self._next_seq
        self._next_seq += 1
        # ---------------------------------

        async with self._write_lock:
            self._writer.write(
                struct.pack(
                    '!IHII',
                    4 + 2 + 4 + 4 + len(cmd),
                    EXECUTE,
                    seq_num,
                    len(cmd)
                ) + cmd
            )
            await self._writer.drain()

        message = await self._wait_message(seq_num)
        msgSize, msgType, msgID, retCode, retLen = struct.unpack('!IHIII', message[:18])
        retStr = message[18:].decode('utf-8')
        assert(len(retStr) == retLen and msgType == EXECUTE_RESP and msgID == seq_num)
        return (retCode, retStr)

    async def execute_no_output(self, cmd):
        # TODO: check proper encoding
        cmd = cmd.encode('utf-8')

        # -- naturally ensures atomicity --
        seq_num = self._next_seq
        self._next_seq += 1
        # ---------------------------------

        async with self._write_lock:
            self._writer.write(
                struct.pack(
                    '!IHII',
                    4 + 2 + 4 + 4 + len(cmd),
                    EXECUTE_NO_OUTPUT,
                    seq_num,
                    len(cmd)
                ) + cmd
            )
            await self._writer.drain()

        message = await self._wait_message(seq_num)
        msgSize, msgType, msgID, retCode = struct.unpack('!IHII', message)
        assert(msgType == EXECUTE_NO_OUTPUT_RESP and msgID == seq_num)
        return retCode

    async def close(self):
        self._writer.close()
        self._client = None
        await self._writer.wait_closed()

# https://stackoverflow.com/questions/53779956/why-should-asyncio-streamwriter-drain-be-explicitly-called
class ExecutorServer:
    """Information format:
    MessageFormat:
        U32 MessageSize # Including this
        U16 MessageType
        U32 MessageID   # used to distinguish req-resp pair
        <Payload>

    Request <Payload>:
    - PING: MessageType = 1
        <No extra content>
    - EXECUTE: MessageType = 2
        U32 CommandLength
        String Command
    - EXECUTE_NO_OUTPUT: MessageType = 3
        U32 CommandLength
        String Command

    Response <Payload>:
    - PONG: MessageType = 100
        <No extra content>
    - EXECUTE_RESP: MessageType = 101
        U32 ResultReturnCode
        U32 ResultLength
        String Result
    - EXECUTE_NO_OUTPUT_RESP: MessageType = 102
        U32 ResultReturnCode
    
    """

    def __init__(self, host='0.0.0.0', port=11451):
        self._host = host
        self._port = port
        self._server = None
        # Prevent from multiple handlers writing simutaenously
        # - (In theory no await => no reschedule, but who knows?)
        self._write_lock = None
    
    def serve_forever(self):
        asyncio.get_event_loop().run_until_complete(self.__run())

    async def __run(self):
        self._server = await asyncio.start_server(
            self.request_handler, self._host, self._port)
        self._write_lock = asyncio.Lock()

        addr = self._server.sockets[0].getsockname()
        logger.info(f'Serving on {addr}')

        async with self._server:
            await self._server.serve_forever()

    async def ping_handler(self, writer, msgID):
        resp = struct.pack('!IHI', 4 + 2 + 4, PONG, msgID)

        # Uncomment to see the true power of async (x)
        # await asyncio.sleep(1)

        async with self._write_lock:
            writer.write(resp)
            await writer.drain()

        #writer.close()
        #await writer.wait_closed()

    async def execute_handler(self, writer, msgID, cmd):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=None
        )

        stdout, _ = await proc.communicate()

        resp = struct.pack(
            '!IHIII',
            4 + 2 + 4 + 4 + 4 + len(stdout),
            EXECUTE_RESP,
            msgID,
            proc.returncode,
            len(stdout)
        ) + stdout

        async with self._write_lock:
            writer.write(resp)
            await writer.drain()

    async def execute_no_output_handler(self, writer, msgID, cmd):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=None
        )

        await proc.wait()
    
        resp = struct.pack(
            '!IHII',
            4 + 2 + 4 + 4,
            EXECUTE_NO_OUTPUT_RESP,
            msgID,
            proc.returncode
        )

        async with self._write_lock:
            writer.write(resp)

    # actually await is 33% faster in 10000 PING-PONG test, but we need duplex
    # so we use create_task
    async def request_handler(self, reader, writer):
        addr = writer.get_extra_info('peername')
        wblow, wbhigh = writer.transport.get_write_buffer_limits()
        logger.info(f"New clients from [{addr}]: wblimit=({wblow}, {wbhigh})")

        try:
            while True:
                # (Read a message packet at once!)
                header = await reader.readexactly(10)
                msgSize, msgType, msgID = struct.unpack('!IHI', header)
                if msgType == 1:    # PING
                    #await self.ping_handler(writer, msgID)
                    asyncio.create_task(self.ping_handler(writer, msgID))

                elif msgType == 2:  # EXECUTE
                    cmdLen, = struct.unpack('!I', await reader.readexactly(4))
                    cmd = await reader.readexactly(cmdLen)  # bytes are OK
                    asyncio.create_task(self.execute_handler(writer, msgID, cmd))

                elif msgType == 3:  # EXECUTE_NO_OUTPUT
                    cmdLen, = struct.unpack('!I', await reader.readexactly(4))
                    cmd = (await reader.readexactly(cmdLen))
                    asyncio.create_task(self.execute_no_output_handler(writer, msgID, cmd))

                else:
                    raise Exception("Invalid Message Type")
        except asyncio.exceptions.IncompleteReadError as e:
            logger.info(f"Client [{addr}] have left.")
            # TODO: cleanup spawned subprocesses?
        

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} mode, mode := server | client")
        print(f"When in client mode, this program connects to localhost")
        sys.exit(1)
    
    if sys.argv[1] == "server":
        logging.basicConfig(
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO
        )

        server = ExecutorServer()
        server.serve_forever()
    elif sys.argv[1] == "client":
        async def run_ping():
            client = ExecutorClient('localhost')
            await client.connect()
            call_time = await client.ping()
            print(f"PING-PONG time: {call_time * 1000} msec.")

            call_time = 0
            for i in range(0, 10000):
                call_time += await client.ping()
            print(f"10000 sequential PING-PONG time: {call_time * 1000} msec.")

            start_time = time.perf_counter()
            call_time = functools.reduce(
                lambda x, y: x+y,
                await asyncio.gather(*[client.ping() for i in range(0, 10000)])
            )
            end_time = time.perf_counter()
            print(f"10000 parallel PING-PONG clock time: {call_time * 1000} msec.")
            print(f"10000 parallel PING-PONG real time: {(end_time - start_time) * 1000} msec.")
            
            start_time = time.perf_counter()
            call_time = functools.reduce(
                lambda x, y: x+y,
                await asyncio.gather(*[client.ping() for i in range(0, 256)])
            )
            end_time = time.perf_counter()
            print(f"256 parallel PING-PONG clock time: {call_time * 1000} msec.")
            print(f"256 parallel PING-PONG real time: {(end_time - start_time) * 1000} msec.")

            cmd = 'echo "Yajyuusenpai!!"'
            retCode, retStr = await client.execute(cmd)
            print(f"Remote execute test for [{cmd}]: retcode={retCode}, out='''{retStr}'''")

            cmd = '/bin/false'
            retCode = await client.execute_no_output(cmd)
            print(f"Remote execute test for [{cmd}]: retcode={retCode}")

            cmd = '/bin/true'
            retCode = await client.execute_no_output(cmd)
            print(f"Remote execute test for [{cmd}]: retcode={retCode}")

            cmd = '/bin/false'
            start_time = time.perf_counter()
            ret_sum = functools.reduce(
                lambda x, y: x+y,
                await asyncio.gather(*[client.execute_no_output(cmd) for i in range(0, 1000)])
            )
            end_time = time.perf_counter()
            print(f"1000 remote execute no output for [{cmd}] retCode Sum: {ret_sum} ")
            print(f"1000 remote execute no output for [{cmd}] realtime: {(end_time - start_time) * 1000} msec.")

            await client.close()

        asyncio.run(run_ping())