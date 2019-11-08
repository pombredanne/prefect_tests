from prefect import Flow, Parameter
from prefect.tasks.docker import PullImage, CreateContainer, StopContainer, StartContainer, ListContainers

# Task definitions
pull = PullImage()
create = CreateContainer()
list_containers = ListContainers()
# The StarContainer task uses the docker python API but doesn't allow extra args like port mappings
start = StartContainer()
stop = StopContainer()

# Flow definitions
with Flow('Pull and bring containers up') as images_up_flow:
    repository = Parameter('repository')
    image_tag = Parameter('image_tag')
    pull_task = pull(repository=repository, tag=image_tag)

    container_count = Parameter('amount')
    image_set = [repository] * container_count
    create_tasks = create.map(image_name=image_set, upstream_tasks=[pull_task])

    start_tasks = start.map(container_id=create_tasks)

with Flow('Bring running containers down') as containers_down_flow:
    # List of dicts which contain the container_id key
    list_task = list_containers()

    stop_task = stop.map(list_task)

# Entry test point

if __name__ == '__main__':
    # This flow pulls the mongo:latest image and launches 3 containers from it
    images_up_flow.visualize()
    # images_up_flow.run(dict(repository='mongo', image_tag='latest', amount=3))

    # This flow stops all running containers
    containers_down_flow.visualize()
    # containers_down_flow.run()

