import asyncio
import queue
import janus


# At least one task has failed, so we need to clean up.
# A task completed if cancelled is False, AND exception is None.
# A task has an exception if cancelled is False, AND exception is not None.
# A task is cancelled if cancelled is True. (If you check exception, an exception will be raised.)
def get_task_status(task):
    if task.done() is False:
        return("running", "")
    elif task.cancelled():
        return ("cancelled", "")
    elif task.exception() is not None:
        return ("exception", task.exception())
    else:
        return ("completed", task.result())


async def queue_processor(async_q: janus.AsyncQueue[int], crash: bool = True):
    # Continuously look for events in the queue and process them.
    # The queue will be closed outside of this function when the replay is done.
    while async_q.closed is False:
        while not async_q.empty():
            event = await async_q.get()
            print(f"Processed event: {event}")
            async_q.task_done()

        # If there was nothing on the queue, then wait for a second before checking again.
        await asyncio.sleep(1)

        if crash:
            crash_queue_processor()
    print("======> Processor finished")

        

async def queue_injector(queue: janus.Queue[int], crash: bool = True, join_and_close_queue: bool = False):
    async_q = queue.async_q
    await asyncio.sleep(2)
    for i in range(10):
        await async_q.put(i)
        print(f"Injected {i}")
    
    if crash:
        await crash_queue_injector()
    
    print("======> Injector finished")
    if join_and_close_queue:
        print("======> Injector waiting for queue to finish processing")
        await async_q.join()
        print("======> Injector closing the queue")
        queue.close()
        await queue.wait_closed()
    print("======> Injector exiting")



# This case will have exceptions caught, but it's pretty ugly
async def main():
    queue = janus.Queue()

    task_processor = asyncio.create_task(queue_processor(queue.async_q))
    task_injector = asyncio.create_task(queue_injector(queue, crash=False))

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


async def main_task_group_exceptions():
    queue = janus.Queue()

    async def fast_task():
        return

    try:
        async with asyncio.TaskGroup() as tg:
            task_fast = tg.create_task(fast_task())
            task_processor = tg.create_task(queue_processor(queue.async_q))

            # TaskGroup will not exit until all tasks are done, so we have to 
            # have the injector wait for the queue to be processed and then close it
            # so the processor can see that there is no more work.
            # This sucks because there are too many dependencies between both tasks.
            # This is a simple example, so its fine, but in a more complex system
            # this is no bueno.
            task_injector = tg.create_task(queue_injector(queue, join_and_close_queue=True))
    except* Exception as e:
        # We'll just print out the status of each task.
        (status, value) = get_task_status(task_processor)
        print(f"Processor: {status} - {value}")

        (status, value) = get_task_status(task_injector)
        print(f"Injector: {status} - {value}")
        
        (status, value) = get_task_status(task_fast)
        print(f"Fast Task: {status} - {value}")
    finally:
        # If the queue was not closed, then close it and wait for it to be closed.
        # If the above succeeds, then queue will always be closed, since the task_injector 
        # is responsible for closing it, so this isn't actually necessary to be in finally, and could just
        # be in except.
        if queue.closed is False:
            print("Queue was not closed, so main loop is closing it")
            queue.close()
            await queue.wait_closed()


async def main_async_futures_exceptions():
    loop = asyncio.get_event_loop()
    queue = janus.Queue()

    task_processor = asyncio.create_task(queue_processor(queue.async_q))
    future_processor = loop.create_future()
    task_processor.add_done_callback(future_processor.set_result)
    
    task_injector = asyncio.create_task(queue_injector(queue, crash=False))
    future_injector = loop.create_future()
    task_injector.add_done_callback(future_injector.set_result)

    # Wait for the injector to finish
    await future_injector
    exc = task_injector.exception()

    if exc is not None:
        print("task_injector Exception:", exc)
        # Need to shut down processor and queue
        queue.close()
        await queue.wait_closed()
        await future_processor
        exc = task_processor.exception()
        if exc is not None:
            print("task_processor Exception:", exc)
        return

    print("======> Injector finished")
   
    ########################################################################################
    # PROBLEM: If Injector finishes, but processor crashes, then the queue will never empty,
    # And we'll never get past here.
    ########################################################################################

    # Now wait for the queue to be finished processing
    await queue.async_q.join()

    # Queue is empty, so close it
    queue.close()
    await queue.wait_closed()

    # Queue is closed, so wait for processor to detect this and finish
    await future_processor
    exc = task_processor.exception()
    if exc is not None:
        print("task_processor Exception:", exc)

    # At least one task has failed, so we need to clean up.
        # A task completed if cancelled is False, AND exception is None.
        # A task has an exception if cancelled is False, AND exception is not None.
        # A task is cancelled if cancelled is True. (If you check exception, an exception will be raised.)
        def get_task_status(task):
            if task.cancelled():
                return ("cancelled", None)
            elif task.exception() is not None:
                return ("exception", task.exception())
            else:
                return ("completed", task.result())

async def main_async_gather_exceptions():
    queue = janus.Queue()

    task_processor = asyncio.create_task(queue_processor(queue.async_q))
    task_injector = asyncio.create_task(queue_injector(queue, crash=False, join_and_close_queue=True))

    tasks = [task_processor, task_injector]

    # Wait for the injector to finish
    group = asyncio.gather(*tasks)

    try:
        await group
    except Exception as e:
        # We'll just print out the status of each task.
        (status, value) = get_task_status(task_processor)
        print(f"Processor: {status} - {value}")

        (status, value) = get_task_status(task_injector)
        print(f"Injector: {status} - {value}")

        # Cancel all running tasks
        for task in tasks:
            if task.done() is False:
                task.cancel()
                await asyncio.sleep(1)
        
        # Close the queue and then return
        queue.close()
        await queue.wait_closed()
        return

    # Now wait for the queue to be finished processing
    group = queue.async_q.join()

    # Queue is empty, so close it
    queue.close()
    await queue.wait_closed()

    # Queue is closed, so wait for processor to detect this and finish
    await asyncio.gather(task_processor)


if __name__ == "__main__":
    # Regular try/except handling
    #asyncio.run(main())

    # Exception handling with TaskGroup
    # This requires Python >= 3.11
    #asyncio.run(main_task_group_exceptions())

    # Exception handling with futures
    #asyncio.run(main_async_futures_exceptions())

    # Exception handling with gather
    asyncio.run(main_async_gather_exceptions())
