# First-time build can take up to 10 mins.
FROM apache/airflow:2.10.2

ENV AIRFLOW_HOME=/opt/airflow
ENV CLOUD_SDK_VERSION=415.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

# Utiliser l'utilisateur root pour installer vim et les dépendances système
USER root
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    vim \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Ajouter la clé publique de Google Cloud SDK
RUN curl -sSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -

# Ajouter le dépôt Google Cloud SDK à la liste des sources
RUN echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Installer Google Cloud SDK
RUN apt-get update -qq && apt-get install -y --no-install-recommends google-cloud-sdk

# Vérifier que gcloud est installé
RUN gcloud --version

# Copier les requirements pour installer les paquets Python
COPY requirements.txt .
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
USER root

# Définir le répertoire de travail
WORKDIR $AIRFLOW_HOME

# Copier et donner les permissions sur les scripts
COPY scripts scripts
RUN chmod +x scripts

# Revenir à l'utilisateur airflow
USER $AIRFLOW_UID
