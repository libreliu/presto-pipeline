"""Local executor"""

import asyncio
import logging

logger = logging.getLogger(__name__)

class LocalExecutor:
    def __init__(self):
        pass

    async def execute(self, cmd):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=None
        )

        stdout, _ = await proc.communicate()
        return (proc.returncode, stdout.decode('utf-8'))
    
    async def execute_no_output(self, cmd):
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=None
        )

        await proc.wait()
        return proc.returncode