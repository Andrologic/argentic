"""Relay HTTP proxy traffic from private loopback to the mounted Unix socket."""

import asyncio
import sys


async def copy(source, destination):
    while data := await source.read(16384):
        destination.write(data)
        await destination.drain()
    destination.write_eof()


async def main():
    connections = set()

    async def relay(reader, writer):
        async with asyncio.timeout(120):
            upstream, out = await asyncio.open_unix_connection("/run/argentic-proxy.sock")
            try:
                async with asyncio.TaskGroup() as group:
                    group.create_task(copy(reader, out))
                    group.create_task(copy(upstream, writer))
            finally:
                out.close()

    async def serve(reader, writer):
        try:
            await relay(reader, writer)
        except (OSError, TimeoutError, ExceptionGroup):
            pass
        finally:
            writer.close()

    def accept(reader, writer):
        if len(connections) >= 16:
            writer.close()
            return
        task = asyncio.create_task(serve(reader, writer))
        connections.add(task)
        task.add_done_callback(connections.discard)

    async with await asyncio.start_server(accept, "127.0.0.1", int(sys.argv[1])):
        child = await asyncio.create_subprocess_exec(*sys.argv[2:])
        return await child.wait()


status = asyncio.run(main())
sys.exit(status if status >= 0 else 128 - status)
