"""Relay HTTP proxy traffic from private loopback to the mounted Unix socket."""

import sys

if sys.version_info < (3, 11):
    sys.exit("Argentic Linux shell network relay requires system Python 3.11 or later")

import asyncio
import select


async def copy(source, destination):
    while data := await source.read(16384):
        destination.write(data)
        await destination.drain()
    destination.write_eof()


async def main():
    connections = set()

    async def relay(reader, writer):
        upstream, out = await asyncio.open_unix_connection("/run/argentic-proxy.sock")

        async def disconnect():
            poll = select.poll()
            poll.register(writer.get_extra_info("socket").fileno(), select.POLLRDHUP)
            while not poll.poll(0):
                await asyncio.sleep(0.05)
            out.transport.abort()

        watch = asyncio.create_task(disconnect())
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(copy(reader, out))
                group.create_task(copy(upstream, writer))
        finally:
            watch.cancel()
            out.close()

    async def serve(reader, writer):
        try:
            await relay(reader, writer)
        except (OSError, ExceptionGroup):
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
