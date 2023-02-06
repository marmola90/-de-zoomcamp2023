from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from etl_web_to_gcs import etl_web_to_gcs

github_block = GitHub.load("zoom-gitbloack")

github_dep=Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name='github_flow',
    storage=github_block
)

if __name__=='__main__':
    github_dep.apply()