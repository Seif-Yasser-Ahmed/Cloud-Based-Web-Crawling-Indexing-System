from mpi4py import MPI
import time
import logging
from common.utils import load_seed_urls
# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - Master - %(levelname)s - %(message)s')


def master_process():
    """
    Master node logic:
    - Distributes URLs to crawlers
    - Monitors crawler responses
    - Handles basic task queue and fault signals
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    crawler_nodes = size - 2  # Assuming 1 master, 1+ crawlers, 1 indexer
    indexer_nodes = 1

    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error(
            "Not enough nodes. Need at least 1 master, 1 crawler, 1 indexer.")
        return

    active_crawler_nodes = list(range(1, 1 + crawler_nodes))
    active_indexer_nodes = list(range(1 + crawler_nodes, size))

    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    seed_urls = ["http://example.com", "http://example.org"]
    urls_to_crawl_queue = seed_urls.copy()
    task_count = 0
    crawler_tasks_assigned = 0

    while urls_to_crawl_queue or crawler_tasks_assigned > 0:
        # Check for messages from crawlers
        if crawler_tasks_assigned > 0 and comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            message_source = status.Get_source()
            message_tag = status.Get_tag()
            message_data = comm.recv(source=message_source, tag=message_tag)

            if message_tag == 1:  # Extracted URLs returned
                crawler_tasks_assigned -= 1
                new_urls = message_data
                if new_urls:
                    urls_to_crawl_queue.extend(new_urls)
                logging.info(
                    f"Received {len(new_urls)} URLs from Crawler {message_source}")
            elif message_tag == 99:  # Heartbeat/status
                logging.info(
                    f"Heartbeat from Crawler {message_source}: {message_data}")
            elif message_tag == 999:  # Error report
                logging.error(
                    f"Error from Crawler {message_source}: {message_data}")
                crawler_tasks_assigned -= 1  # Assume task failed

        # Assign new tasks
        while urls_to_crawl_queue and crawler_tasks_assigned < crawler_nodes:
            url_to_crawl = urls_to_crawl_queue.pop(0)
            assigned_rank = active_crawler_nodes[crawler_tasks_assigned % len(
                active_crawler_nodes)]
            task_id = task_count
            task_count += 1
            comm.send(url_to_crawl, dest=assigned_rank, tag=0)
            crawler_tasks_assigned += 1
            logging.info(
                f"Assigned Task {task_id}: {url_to_crawl} -> Crawler {assigned_rank}")
            time.sleep(0.1)

        time.sleep(1)

    logging.info("Master node finished all task distribution.")
    print("Master Node Finished.")


if __name__ == '__main__':
    master_process()
