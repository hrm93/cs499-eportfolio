# New Test to check progress bar (rich.progress)
from time import sleep
from rich.progress import Progress

def test_progress_bar(capsys):
    total_tasks = 6
    with Progress() as progress:
        task = progress.add_task("[cyan]Processing reports...", total=total_tasks)
        for _ in range(total_tasks):
            sleep(0.1)  # shorter sleep to keep tests fast
            progress.advance(task)
    print("Progress bar test complete!")

    # Capture console output and verify the completion message is printed
    captured = capsys.readouterr()
    assert "Progress bar test complete!" in captured.out
