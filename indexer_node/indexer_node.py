from mpi4py import MPI
import time
import logging

# Import your indexing library when implemented (e.g., Whoosh, Elasticsearch)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - Indexer - %(levelname)s - %(message)s')


def indexer_process():
    """
    Indexer node logic:
    - Receives page content from crawlers
    - Processes and indexes the content
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Indexer node started with rank {rank} of {size}")

    # Initialize index or DB connections here

    while True:
        status = MPI.Status()
        content_to_index = comm.recv(
            source=MPI.ANY_SOURCE, tag=2, status=status)
        source_rank = status.Get_source()

        if not content_to_index:
            logging.info(f"Indexer {rank} received shutdown signal. Exiting.")
            break

        logging.info(
            f"Indexer {rank} received content from Crawler {source_rank}.")

        try:
            # Simulate indexing (replace this with real logic)
            time.sleep(1)

            logging.info(
                f"Indexer {rank} indexed content from Crawler {source_rank}.")
            comm.send(
                f"Indexer {rank} - Indexed content from Crawler {source_rank}", dest=0, tag=99)

        except Exception as e:
            logging.error(
                f"Error indexing content from Crawler {source_rank}: {e}")
            comm.send(f"Indexer {rank} - Error indexing: {e}", dest=0, tag=999)


if __name__ == '__main__':
    indexer_process()
