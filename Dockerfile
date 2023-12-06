FROM public.ecr.aws/amazonlinux/amazonlinux:2023 as osml_tile_server

# Only override if you're using a mirror with a cert pulled in using cert-base as a build parameter
ARG BUILD_CERT=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
ARG PIP_INSTALL_LOCATION=https://pypi.org/simple/

# Give sudo permissions
USER root

# Set working directory to home
WORKDIR /home

# Configure, update, and refresh yum enviornment
RUN yum update -y && yum clean all && yum makecache && yum install -y wget shadow-utils

# Install miniconda
ARG MINICONDA_VERSION=Miniconda3-latest-Linux-x86_64
ARG MINICONDA_URL=https://repo.anaconda.com/miniconda/${MINICONDA_VERSION}.sh
RUN wget -c ${MINICONDA_URL} \
    && chmod +x ${MINICONDA_VERSION}.sh \
    && ./${MINICONDA_VERSION}.sh -b -f -p /opt/conda \
    && rm ${MINICONDA_VERSION}.sh \
    && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh

# Add conda to the path so we can execute it by name
ENV PATH=/opt/conda/bin:$PATH

# Set all the ENV vars needed for build
ENV CONDA_TARGET_ENV=osml_tile_server
ENV CC="clang"
ENV CXX="clang++"
ENV ARCHFLAGS="-arch x86_64"
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/opt/conda/lib/:/opt/conda/bin:/usr/include:/usr/local/"
ENV PROJ_LIB=/opt/conda/share/proj

# Copy our conda env configuration
COPY environment.yml .

# Create the conda env
RUN conda env create

# Create /entry.sh which will be our new shell entry point/
# This performs actions to configure the environment
# before starting a new shell (which inherits the env).
# The exec is important as this allows signals to passpw.
RUN     (echo '#!/bin/bash' \
    &&   echo '__conda_setup="$(/opt/conda/bin/conda shell.bash hook 2> /dev/null)"' \
    &&   echo 'eval "$__conda_setup"' \
    &&   echo 'conda activate "${CONDA_TARGET_ENV:-base}"' \
    &&   echo '>&2 echo "ENTRYPOINT: CONDA_DEFAULT_ENV=${CONDA_DEFAULT_ENV}"' \
    &&   echo 'exec "$@"'\
    ) >> /entry.sh && chmod +x /entry.sh

# Tell the docker build process to use this for RUN.
# the default shell on Linux is ["/bin/sh", "-c"], and on Windows is ["cmd", "/S", "/C"]
SHELL ["/entry.sh", "/bin/bash", "-c"]

# Configure .bashrc to drop into a conda env and immediately activate our TARGET env
RUN conda init && echo 'conda activate "${CONDA_TARGET_ENV:-base}"' >>  ~/.bashrc

# Copy our lcoal application source into the container
ADD . osml-tile-server

# Install the model runner application from source
RUN python3 -m pip install osml-tile-server/

# Clean up the conda install
RUN conda clean -afy

# Set up a health check at that port
HEALTHCHECK NONE

# Make sure we expose our ports
EXPOSE 8080

# Set up a user to run the container as and assume it
RUN adduser tileserver
RUN chown -R tileserver:tileserver osml-tile-server/
USER tileserver

# Set the entry point script
ENTRYPOINT ["/entry.sh", "/bin/bash", "-c", "uvicorn --host 0.0.0.0 --port 8080 aws.osml.tile_server.main:app"]
