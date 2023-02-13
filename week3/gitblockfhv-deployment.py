from prefect.deployments import Deployment
from prefect.filesystems import GitHub
#from prefect.infrastructure.docker import DockerContainer
from etl_web_to_gcs_fhv import etl_parent_flow

github_block = GitHub.load("zoom-gitbloack")
#docker_block = DockerContainer.load("zoom")

github_dep=Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='fhvData',
    storage=github_block
    # infrastructure=docker_block
)


if __name__=='__main__':
    github_dep.apply()