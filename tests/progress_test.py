from time import sleep
from rich.progress import Progress

def test_progress_bar():
    total_tasks = 6
    with Progress() as progress:
        task = progress.add_task("[cyan]Processing reports...", total=total_tasks)
        for _ in range(total_tasks):
            sleep(0.5)  # simulate work
            progress.advance(task)
    print("Progress bar test complete!")

if __name__ == "__main__":
    test_progress_bar()
