FROM registry.gitlab.com/leipziggiesst/treedata/conda:latest
COPY ./env.yml env.yml
RUN conda env create -f env.yml
RUN echo 'conda activate treedata' >> /root/.bashrc
ENTRYPOINT [ "" ]
