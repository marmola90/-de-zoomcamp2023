from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect.infrastructure.docker import DockerContainer
from etl_web_to_gcs import etl_web_to_gcs

github_block = GitHub.load("zoom-gitbloack")
docker_block = DockerContainer.load("zoom")

github_dep=Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name='github_flow',
    storage=github_block,
    infrastructure=docker_block
)


if __name__=='__main__':
    github_dep.apply()