FROM python:latest

# ARG CUR_USER=ali
# RUN addgroup $CUR_USER && adduser $CUR_USER --group $CUR_USER
# USER $CUR_USER

ARG USERNAME=ali
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME

RUN pip install pgcli && pip install jupyterlab && pip install pandas && pip install sqlalchemy && pip install psycopg2

USER $USERNAME
WORKDIR /home/$USERNAME
RUN mkdir app