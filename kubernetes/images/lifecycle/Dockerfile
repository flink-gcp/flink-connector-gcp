# Copyright 2026 The flink-gcp authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Python 3.12.14 slim-bookworm, linux/amd64.
FROM python@sha256:9c47360a2a0355e2da18516d0b1c2126ec22c195d2185e97347c9d98398c5bef
ADD --checksum=sha256:12e97f9d23a9f6cbb87b89becd6bd291e1a858a3379a4e11e2c822c4c1530052 --chmod=0555 \
    https://dl.k8s.io/release/v1.35.7/bin/linux/amd64/kubectl /usr/local/bin/kubectl
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/tmp
USER 65532:65532
WORKDIR /tmp
ENTRYPOINT ["python3"]
