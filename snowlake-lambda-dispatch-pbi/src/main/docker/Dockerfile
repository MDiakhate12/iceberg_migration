FROM public.ecr.aws/lambda/python:3.12
LABEL Name="${docker.image.name}"
LABEL maintainer="Dev Snow <dev-snow@sogelink.fr>"

COPY python/* ${LAMBDA_TASK_ROOT}/

COPY venv/ ${LAMBDA_TASK_ROOT}/venv/
ENV PYTHONPATH "${PYTHONPATH}:${LAMBDA_TASK_ROOT}/venv/"

CMD [ "handler.main" ]
LABEL Version="${project.version}"
