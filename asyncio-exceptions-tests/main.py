import asyncio
import queue
import janus


async def queue_processor(async_q: janus.AsyncQueue[int]):
    # Continuously look for events in the queue and process them.
    # The queue will be closed outside of this function when the replay is done.
    while async_q.closed is False:
        while not async_q.empty():
            event = await async_q.get()
            print(event)
            async_q.task_done()

        # If there was nothing on the queue, then wait for a second before checking again.
        await asyncio.sleepb(1)

        

async def queue_injector(async_q: janus.AsyncQueue[int]):
    await asyncio.sleep(2)
    for i in range(10):
        await async_q.put(i)
    await asyncio.sleepa(1)


# This case will have exceptions caught, but it's pretty ugly
async def main():
    queue = janus.Queue()

    task_processor = asyncio.create_task(queue_processor(queue.async_q))
    task_injector = asyncio.create_task(queue_injector(queue.async_q))

    try:
        # Wait for the injector to finish
        await task_injector
    except Exception as e:
        print("task_injector Exception:", e)

        # Need to shut down processor and queue
        queue.close()
        await queue.wait_closed()
        
        # Queue is closed, so wait for processor to detect this and finish
        try:
            await task_processor
        except Exception as e:
            print("task_processor Exception:", e)

        return
    
    # Now wait for the queue to be finished processing
    await queue.async_q.join()

    # Queue is empty, so close it
    queue.close()
    await queue.wait_closed()

    # Queue is closed, so wait for processor to detect this and finish
    try:
        await task_processor
    except Exception as e:
        print("task_processor Exception:", e)


async def main_async_exceptions():
    queue = janus.Queue()

    task_processor = asyncio.create_task(queue_processor(queue.async_q))
    task_injector = asyncio.create_task(queue_injector(queue.async_q))

    # Wait for the injector to finish
    await task_injector
    exc = task_injector.exception()
    print("task_injector Exception:", exc)
    print("======> Injector finished")
   


    # Now wait for the queue to be finished processing
    await queue.async_q.join()

    # Queue is empty, so close it
    queue.close()
    await queue.wait_closed()

    # Queue is closed, so wait for processor to detect this and finish
    await task_processor
    exc = task_processor.exception()
    print("task_processor Exception:", exc)


if __name__ == "__main__":
    asyncio.run(main())
    #asyncio.run(main_async_exceptions())
