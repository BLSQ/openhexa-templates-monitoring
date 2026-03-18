from openhexa.sdk.client import openhexa
import pandas as pd


def extract_data():
    pipelines_data = []
    runs_data = []

    workspaces_response = openhexa.workspaces()

    for workspace in workspaces_response.items:
        workspace_slug = workspace.slug

        pipelines_response = openhexa.pipelines(
            workspace_slug=workspace_slug,
            page=1,
            per_page=10
        )

        for i in range(pipelines_response.total_pages):
            current_page = i + 1

            pipelines_response = openhexa.pipelines(
                workspace_slug=workspace_slug,
                page=current_page,
                per_page=10
            )

            for pipeline in pipelines_response.items:
                pipeline_details = openhexa.pipeline(
                    workspace_slug=workspace_slug,
                    pipeline_code=pipeline.code,
                )

                current_version = pipeline_details.current_version

                created_at = None
                if current_version is not None:
                    created_at = current_version.created_at
                
                source_template = pipeline_details.source_template

                template_id = None
                if source_template is not None:
                    template_id = source_template.name



                pipelines_data.append({
                    "pipeline_id": pipeline_details.id,
                    "pipeline_code": pipeline_details.code,
                    "name": pipeline_details.name,
                    "type": pipeline_details.type.name,
                    "workspace_slug": workspace_slug,
                    "template_id": template_id,
                    "created_at": created_at
                })

                # Extract runs
                for run in pipeline_details.runs.items:
                    runs_data.append({
                        "run_id": run.id,
                        "pipeline_id": pipeline_details.id,
                        "status": run.status.name,
                        "execution_date": run.execution_date,
                        "user": str(run.user)
                    })

    return pd.DataFrame(pipelines_data), pd.DataFrame(runs_data)