from invoke import task
import invoke

@task()
def serve(ctx):
    """
    Serve the application
    """
    args = [
        'dev_appserver.py',
        '--python_virtualenv_path=./venv',
        './demo'
    ]
    invoke.run(" ".join(args), env={'APPLICATION_ID': 'mapreduce-demo'})


@task()
def test(ctx):
    """ Run the tests """
    invoke.run("python test/mapreduce/run_tests.py")
