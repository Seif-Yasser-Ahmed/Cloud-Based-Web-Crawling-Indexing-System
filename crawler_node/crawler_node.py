from mpi4py import MPI
import time
import logging

# Import your actual crawling libraries when implementing (e.g., requests, bs4)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - Crawler - %(levelname)s - %(message)s')


def crawler_process():
    """
    Crawler node logic:
    - Receives a URL from the master
    - Fetches and parses the page
    - Extracts new URLs and sends them back to the master
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    logging.info(f"Crawler node started with rank {rank} of {size}")

    while True:
        status = MPI.Status()
        url_to_crawl = comm.recv(source=0, tag=0, status=status)

        if not url_to_crawl:
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")

        try:
            # Simulate crawling (replace this with real crawling logic)
            time.sleep(2)

            extracted_urls = [
                f"http://example.com/page_from_crawler_{rank}_{i}"
                for i in range(2)
            ]

            logging.info(
                f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")

            # Send new URLs back to master
            comm.send(extracted_urls, dest=0, tag=1)

            # Optionally: send content to indexer
            # extracted_content = f"Sample content from {url_to_crawl}"
            # indexer_rank = ... (e.g., choose indexer rank via round robin)
            # comm.send(extracted_content, dest=indexer_rank, tag=2)

            comm.send(
                f"Crawler {rank} - Completed URL: {url_to_crawl}", dest=0, tag=99)

        except Exception as e:
            logging.error(f"Error crawling {url_to_crawl}: {e}")
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)


if __name__ == '__main__':
    crawler_process()
