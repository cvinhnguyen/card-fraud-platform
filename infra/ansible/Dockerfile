# Ansible control node for Windows hosts (Ansible does not run natively there). The SSH key is mounted at /key and
# copied with the permissions ssh requires (a bind mount from Windows is world-readable).
FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir ansible==14.5.0
WORKDIR /work/infra/ansible
ENV ANSIBLE_CONFIG=/work/infra/ansible/ansible.cfg
ENTRYPOINT ["sh", "-c", "install -D -m 600 /key ~/.ssh/id_ed25519 && exec \"$@\"", "--"]
