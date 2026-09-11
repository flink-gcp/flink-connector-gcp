// Generated from the Apache Flink Kubernetes Operator 1.15.0 chart.
// Regenerate with just tier3-schemas refresh; do not edit by hand.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1beta1

#FlinkBlueGreenDeployment: {
	_embeddedResource
	spec?: {
		ingress?: {
			annotations?: [string]: string
			className?: string
			labels?: [string]: string
			template?: string
			tls?: [...{
				hosts?: [...string]
				secretName?: string
			}]
		}
		configuration?: [string]: string
		template?: {
			metadata?: {
				annotations?: [string]: string
				creationTimestamp?:          string
				deletionGracePeriodSeconds?: int
				deletionTimestamp?:          string
				finalizers?: [...string]
				generateName?: string
				generation?:   int
				labels?: [string]: string
				managedFields?: [...{
					apiVersion?: string
					fieldsType?: string
					fieldsV1?: {}
					manager?:     string
					operation?:   string
					subresource?: string
					time?:        string
				}]
				name?:      string
				namespace?: string
				ownerReferences?: [...{
					apiVersion?:         string
					blockOwnerDeletion?: bool
					controller?:         bool
					kind?:               string
					name?:               string
					uid?:                string
				}]
				resourceVersion?: string
				selfLink?:        string
				uid?:             string
			}
			spec?: {
				flinkConfiguration?: null | bool | number | string | [...] | {
					...
				}
				flinkVersion?:    "v1_13" | "v1_14" | "v1_15" | "v1_16" | "v1_17" | "v1_18" | "v1_19" | "v1_20" | "v2_0" | "v2_1" | "v2_2"
				image?:           string
				imagePullPolicy?: string
				ingress?: {
					annotations?: [string]: string
					className?: string
					labels?: [string]: string
					template?: string
					tls?: [...{
						hosts?: [...string]
						secretName?: string
					}]
				}
				job?: {
					allowNonRestoredState?: bool
					args?: [...string]
					autoscalerResetNonce?:   int
					checkpointTriggerNonce?: int
					entryClass?:             string
					initialSavepointPath?:   string
					jarURI?:                 string
					parallelism?:            int
					savepointRedeployNonce?: int
					savepointTriggerNonce?:  int
					state?:                  "running" | "suspended"
					upgradeMode?:            "last-state" | "savepoint" | "stateless"
				}
				jobManager?: {
					podTemplate?: {
						apiVersion?: string
						kind?:       string
						metadata?: {
							annotations?: [string]: string
							creationTimestamp?:          string
							deletionGracePeriodSeconds?: int
							deletionTimestamp?:          string
							finalizers?: [...string]
							generateName?: string
							generation?:   int
							labels?: [string]: string
							managedFields?: [...{
								apiVersion?: string
								fieldsType?: string
								fieldsV1?: {}
								manager?:     string
								operation?:   string
								subresource?: string
								time?:        string
							}]
							name?:      string
							namespace?: string
							ownerReferences?: [...{
								apiVersion?:         string
								blockOwnerDeletion?: bool
								controller?:         bool
								kind?:               string
								name?:               string
								uid?:                string
							}]
							resourceVersion?: string
							selfLink?:        string
							uid?:             string
						}
						spec?: {
							activeDeadlineSeconds?: int
							affinity?: {
								nodeAffinity?: {
									preferredDuringSchedulingIgnoredDuringExecution?: [...{
										preference?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchFields?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
										}
										weight?: int
									}]
									requiredDuringSchedulingIgnoredDuringExecution?: nodeSelectorTerms?: [...{
										matchExpressions?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
										matchFields?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
									}]
								}
								podAffinity?: {
									preferredDuringSchedulingIgnoredDuringExecution?: [...{
										podAffinityTerm?: {
											labelSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											matchLabelKeys?: [...string]
											mismatchLabelKeys?: [...string]
											namespaceSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											namespaces?: [...string]
											topologyKey?: string
										}
										weight?: int
									}]
									requiredDuringSchedulingIgnoredDuringExecution?: [...{
										labelSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										matchLabelKeys?: [...string]
										mismatchLabelKeys?: [...string]
										namespaceSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										namespaces?: [...string]
										topologyKey?: string
									}]
								}
								podAntiAffinity?: {
									preferredDuringSchedulingIgnoredDuringExecution?: [...{
										podAffinityTerm?: {
											labelSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											matchLabelKeys?: [...string]
											mismatchLabelKeys?: [...string]
											namespaceSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											namespaces?: [...string]
											topologyKey?: string
										}
										weight?: int
									}]
									requiredDuringSchedulingIgnoredDuringExecution?: [...{
										labelSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										matchLabelKeys?: [...string]
										mismatchLabelKeys?: [...string]
										namespaceSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										namespaces?: [...string]
										topologyKey?: string
									}]
								}
							}
							automountServiceAccountToken?: bool
							containers?: [...{
								args?: [...string]
								command?: [...string]
								env?: [...{
									name?:  string
									value?: string
									valueFrom?: {
										configMapKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
										secretKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
									}
								}]
								envFrom?: [...{
									configMapRef?: {
										name?:     string
										optional?: bool
									}
									prefix?: string
									secretRef?: {
										name?:     string
										optional?: bool
									}
								}]
								image?:           string
								imagePullPolicy?: string
								lifecycle?: {
									postStart?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									preStop?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									stopSignal?: string
								}
								livenessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								name?: string
								ports?: [...{
									containerPort?: int
									hostIP?:        string
									hostPort?:      int
									name?:          string
									protocol?:      string
								}]
								readinessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								resizePolicy?: [...{
									resourceName?:  string
									restartPolicy?: string
								}]
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartPolicy?: string
								securityContext?: {
									allowPrivilegeEscalation?: bool
									appArmorProfile?: {
										localhostProfile?: string
										type?:             string
									}
									capabilities?: {
										add?: [...string]
										drop?: [...string]
									}
									privileged?:             bool
									procMount?:              string
									readOnlyRootFilesystem?: bool
									runAsGroup?:             int
									runAsNonRoot?:           bool
									runAsUser?:              int
									seLinuxOptions?: {
										level?: string
										role?:  string
										type?:  string
										user?:  string
									}
									seccompProfile?: {
										localhostProfile?: string
										type?:             string
									}
									windowsOptions?: {
										gmsaCredentialSpec?:     string
										gmsaCredentialSpecName?: string
										hostProcess?:            bool
										runAsUserName?:          string
									}
								}
								startupProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								stdin?:                    bool
								stdinOnce?:                bool
								terminationMessagePath?:   string
								terminationMessagePolicy?: string
								tty?:                      bool
								volumeDevices?: [...{
									devicePath?: string
									name?:       string
								}]
								volumeMounts?: [...{
									mountPath?:         string
									mountPropagation?:  string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
									subPath?:           string
									subPathExpr?:       string
								}]
								workingDir?: string
							}]
							dnsConfig?: {
								nameservers?: [...string]
								options?: [...{
									name?:  string
									value?: string
								}]
								searches?: [...string]
							}
							dnsPolicy?:          string
							enableServiceLinks?: bool
							ephemeralContainers?: [...{
								args?: [...string]
								command?: [...string]
								env?: [...{
									name?:  string
									value?: string
									valueFrom?: {
										configMapKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
										secretKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
									}
								}]
								envFrom?: [...{
									configMapRef?: {
										name?:     string
										optional?: bool
									}
									prefix?: string
									secretRef?: {
										name?:     string
										optional?: bool
									}
								}]
								image?:           string
								imagePullPolicy?: string
								lifecycle?: {
									postStart?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									preStop?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									stopSignal?: string
								}
								livenessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								name?: string
								ports?: [...{
									containerPort?: int
									hostIP?:        string
									hostPort?:      int
									name?:          string
									protocol?:      string
								}]
								readinessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								resizePolicy?: [...{
									resourceName?:  string
									restartPolicy?: string
								}]
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartPolicy?: string
								securityContext?: {
									allowPrivilegeEscalation?: bool
									appArmorProfile?: {
										localhostProfile?: string
										type?:             string
									}
									capabilities?: {
										add?: [...string]
										drop?: [...string]
									}
									privileged?:             bool
									procMount?:              string
									readOnlyRootFilesystem?: bool
									runAsGroup?:             int
									runAsNonRoot?:           bool
									runAsUser?:              int
									seLinuxOptions?: {
										level?: string
										role?:  string
										type?:  string
										user?:  string
									}
									seccompProfile?: {
										localhostProfile?: string
										type?:             string
									}
									windowsOptions?: {
										gmsaCredentialSpec?:     string
										gmsaCredentialSpecName?: string
										hostProcess?:            bool
										runAsUserName?:          string
									}
								}
								startupProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								stdin?:                    bool
								stdinOnce?:                bool
								targetContainerName?:      string
								terminationMessagePath?:   string
								terminationMessagePolicy?: string
								tty?:                      bool
								volumeDevices?: [...{
									devicePath?: string
									name?:       string
								}]
								volumeMounts?: [...{
									mountPath?:         string
									mountPropagation?:  string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
									subPath?:           string
									subPathExpr?:       string
								}]
								workingDir?: string
							}]
							hostAliases?: [...{
								hostnames?: [...string]
								ip?: string
							}]
							hostIPC?:     bool
							hostNetwork?: bool
							hostPID?:     bool
							hostUsers?:   bool
							hostname?:    string
							imagePullSecrets?: [...{
								name?: string
							}]
							initContainers?: [...{
								args?: [...string]
								command?: [...string]
								env?: [...{
									name?:  string
									value?: string
									valueFrom?: {
										configMapKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
										secretKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
									}
								}]
								envFrom?: [...{
									configMapRef?: {
										name?:     string
										optional?: bool
									}
									prefix?: string
									secretRef?: {
										name?:     string
										optional?: bool
									}
								}]
								image?:           string
								imagePullPolicy?: string
								lifecycle?: {
									postStart?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									preStop?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									stopSignal?: string
								}
								livenessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								name?: string
								ports?: [...{
									containerPort?: int
									hostIP?:        string
									hostPort?:      int
									name?:          string
									protocol?:      string
								}]
								readinessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								resizePolicy?: [...{
									resourceName?:  string
									restartPolicy?: string
								}]
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartPolicy?: string
								securityContext?: {
									allowPrivilegeEscalation?: bool
									appArmorProfile?: {
										localhostProfile?: string
										type?:             string
									}
									capabilities?: {
										add?: [...string]
										drop?: [...string]
									}
									privileged?:             bool
									procMount?:              string
									readOnlyRootFilesystem?: bool
									runAsGroup?:             int
									runAsNonRoot?:           bool
									runAsUser?:              int
									seLinuxOptions?: {
										level?: string
										role?:  string
										type?:  string
										user?:  string
									}
									seccompProfile?: {
										localhostProfile?: string
										type?:             string
									}
									windowsOptions?: {
										gmsaCredentialSpec?:     string
										gmsaCredentialSpecName?: string
										hostProcess?:            bool
										runAsUserName?:          string
									}
								}
								startupProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								stdin?:                    bool
								stdinOnce?:                bool
								terminationMessagePath?:   string
								terminationMessagePolicy?: string
								tty?:                      bool
								volumeDevices?: [...{
									devicePath?: string
									name?:       string
								}]
								volumeMounts?: [...{
									mountPath?:         string
									mountPropagation?:  string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
									subPath?:           string
									subPathExpr?:       string
								}]
								workingDir?: string
							}]
							nodeName?: string
							nodeSelector?: [string]: string
							os?: name?:              string
							overhead?: [string]: matchN(>=1, [int, string]) & (int | string)
							preemptionPolicy?:  string
							priority?:          int
							priorityClassName?: string
							readinessGates?: [...{
								conditionType?: string
							}]
							resourceClaims?: [...{
								name?:                      string
								resourceClaimName?:         string
								resourceClaimTemplateName?: string
							}]
							resources?: {
								claims?: [...{
									name?:    string
									request?: string
								}]
								limits?: [string]: matchN(>=1, [int, string]) & (int | string)
								requests?: [string]: matchN(>=1, [int, string]) & (int | string)
							}
							restartPolicy?:    string
							runtimeClassName?: string
							schedulerName?:    string
							schedulingGates?: [...{
								name?: string
							}]
							securityContext?: {
								appArmorProfile?: {
									localhostProfile?: string
									type?:             string
								}
								fsGroup?:             int
								fsGroupChangePolicy?: string
								runAsGroup?:          int
								runAsNonRoot?:        bool
								runAsUser?:           int
								seLinuxChangePolicy?: string
								seLinuxOptions?: {
									level?: string
									role?:  string
									type?:  string
									user?:  string
								}
								seccompProfile?: {
									localhostProfile?: string
									type?:             string
								}
								supplementalGroups?: [...int]
								supplementalGroupsPolicy?: string
								sysctls?: [...{
									name?:  string
									value?: string
								}]
								windowsOptions?: {
									gmsaCredentialSpec?:     string
									gmsaCredentialSpecName?: string
									hostProcess?:            bool
									runAsUserName?:          string
								}
							}
							serviceAccount?:                string
							serviceAccountName?:            string
							setHostnameAsFQDN?:             bool
							shareProcessNamespace?:         bool
							subdomain?:                     string
							terminationGracePeriodSeconds?: int
							tolerations?: [...{
								effect?:            string
								key?:               string
								operator?:          string
								tolerationSeconds?: int
								value?:             string
							}]
							topologySpreadConstraints?: [...{
								labelSelector?: {
									matchExpressions?: [...{
										key?:      string
										operator?: string
										values?: [...string]
									}]
									matchLabels?: [string]: string
								}
								matchLabelKeys?: [...string]
								maxSkew?:            int
								minDomains?:         int
								nodeAffinityPolicy?: string
								nodeTaintsPolicy?:   string
								topologyKey?:        string
								whenUnsatisfiable?:  string
							}]
							volumes?: [...{
								awsElasticBlockStore?: {
									fsType?:    string
									partition?: int
									readOnly?:  bool
									volumeID?:  string
								}
								azureDisk?: {
									cachingMode?: string
									diskName?:    string
									diskURI?:     string
									fsType?:      string
									kind?:        string
									readOnly?:    bool
								}
								azureFile?: {
									readOnly?:   bool
									secretName?: string
									shareName?:  string
								}
								cephfs?: {
									monitors?: [...string]
									path?:       string
									readOnly?:   bool
									secretFile?: string
									secretRef?: name?: string
									user?: string
								}
								cinder?: {
									fsType?:   string
									readOnly?: bool
									secretRef?: name?: string
									volumeID?: string
								}
								configMap?: {
									defaultMode?: int
									items?: [...{
										key?:  string
										mode?: int
										path?: string
									}]
									name?:     string
									optional?: bool
								}
								csi?: {
									driver?: string
									fsType?: string
									nodePublishSecretRef?: name?: string
									readOnly?: bool
									volumeAttributes?: [string]: string
								}
								downwardAPI?: {
									defaultMode?: int
									items?: [...{
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										mode?: int
										path?: string
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
									}]
								}
								emptyDir?: {
									medium?: string
									sizeLimit?: matchN(>=1, [int, string]) & (int | string)
								}
								ephemeral?: volumeClaimTemplate?: {
									metadata?: {
										annotations?: [string]: string
										creationTimestamp?:          string
										deletionGracePeriodSeconds?: int
										deletionTimestamp?:          string
										finalizers?: [...string]
										generateName?: string
										generation?:   int
										labels?: [string]: string
										managedFields?: [...{
											apiVersion?: string
											fieldsType?: string
											fieldsV1?: {}
											manager?:     string
											operation?:   string
											subresource?: string
											time?:        string
										}]
										name?:      string
										namespace?: string
										ownerReferences?: [...{
											apiVersion?:         string
											blockOwnerDeletion?: bool
											controller?:         bool
											kind?:               string
											name?:               string
											uid?:                string
										}]
										resourceVersion?: string
										selfLink?:        string
										uid?:             string
									}
									spec?: {
										accessModes?: [...string]
										dataSource?: {
											apiGroup?: string
											kind?:     string
											name?:     string
										}
										dataSourceRef?: {
											apiGroup?:  string
											kind?:      string
											name?:      string
											namespace?: string
										}
										resources?: {
											limits?: [string]: matchN(>=1, [int, string]) & (int | string)
											requests?: [string]: matchN(>=1, [int, string]) & (int | string)
										}
										selector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										storageClassName?:          string
										volumeAttributesClassName?: string
										volumeMode?:                string
										volumeName?:                string
									}
								}
								fc?: {
									fsType?:   string
									lun?:      int
									readOnly?: bool
									targetWWNs?: [...string]
									wwids?: [...string]
								}
								flexVolume?: {
									driver?: string
									fsType?: string
									options?: [string]: string
									readOnly?: bool
									secretRef?: name?: string
								}
								flocker?: {
									datasetName?: string
									datasetUUID?: string
								}
								gcePersistentDisk?: {
									fsType?:    string
									partition?: int
									pdName?:    string
									readOnly?:  bool
								}
								gitRepo?: {
									directory?:  string
									repository?: string
									revision?:   string
								}
								glusterfs?: {
									endpoints?: string
									path?:      string
									readOnly?:  bool
								}
								hostPath?: {
									path?: string
									type?: string
								}
								image?: {
									pullPolicy?: string
									reference?:  string
								}
								iscsi?: {
									chapAuthDiscovery?: bool
									chapAuthSession?:   bool
									fsType?:            string
									initiatorName?:     string
									iqn?:               string
									iscsiInterface?:    string
									lun?:               int
									portals?: [...string]
									readOnly?: bool
									secretRef?: name?: string
									targetPortal?: string
								}
								name?: string
								nfs?: {
									path?:     string
									readOnly?: bool
									server?:   string
								}
								persistentVolumeClaim?: {
									claimName?: string
									readOnly?:  bool
								}
								photonPersistentDisk?: {
									fsType?: string
									pdID?:   string
								}
								portworxVolume?: {
									fsType?:   string
									readOnly?: bool
									volumeID?: string
								}
								projected?: {
									defaultMode?: int
									sources?: [...{
										clusterTrustBundle?: {
											labelSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											name?:       string
											optional?:   bool
											path?:       string
											signerName?: string
										}
										configMap?: {
											items?: [...{
												key?:  string
												mode?: int
												path?: string
											}]
											name?:     string
											optional?: bool
										}
										downwardAPI?: items?: [...{
											fieldRef?: {
												apiVersion?: string
												fieldPath?:  string
											}
											mode?: int
											path?: string
											resourceFieldRef?: {
												containerName?: string
												divisor?: matchN(>=1, [int, string]) & (int | string)
												resource?: string
											}
										}]
										secret?: {
											items?: [...{
												key?:  string
												mode?: int
												path?: string
											}]
											name?:     string
											optional?: bool
										}
										serviceAccountToken?: {
											audience?:          string
											expirationSeconds?: int
											path?:              string
										}
									}]
								}
								quobyte?: {
									group?:    string
									readOnly?: bool
									registry?: string
									tenant?:   string
									user?:     string
									volume?:   string
								}
								rbd?: {
									fsType?:  string
									image?:   string
									keyring?: string
									monitors?: [...string]
									pool?:     string
									readOnly?: bool
									secretRef?: name?: string
									user?: string
								}
								scaleIO?: {
									fsType?:           string
									gateway?:          string
									protectionDomain?: string
									readOnly?:         bool
									secretRef?: name?: string
									sslEnabled?:  bool
									storageMode?: string
									storagePool?: string
									system?:      string
									volumeName?:  string
								}
								secret?: {
									defaultMode?: int
									items?: [...{
										key?:  string
										mode?: int
										path?: string
									}]
									optional?:   bool
									secretName?: string
								}
								storageos?: {
									fsType?:   string
									readOnly?: bool
									secretRef?: name?: string
									volumeName?:      string
									volumeNamespace?: string
								}
								vsphereVolume?: {
									fsType?:            string
									storagePolicyID?:   string
									storagePolicyName?: string
									volumePath?:        string
								}
							}]
						}
						status?: {
							conditions?: [...{
								lastProbeTime?:      string
								lastTransitionTime?: string
								message?:            string
								observedGeneration?: int
								reason?:             string
								status?:             string
								type?:               string
							}]
							containerStatuses?: [...{
								allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
								allocatedResourcesStatus?: [...{
									name?: string
									resources?: [...{
										health?:     string
										resourceID?: string
									}]
								}]
								containerID?: string
								image?:       string
								imageID?:     string
								lastState?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								name?:  string
								ready?: bool
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartCount?: int
								started?:      bool
								state?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								stopSignal?: string
								user?: linux?: {
									gid?: int
									supplementalGroups?: [...int]
									uid?: int
								}
								volumeMounts?: [...{
									mountPath?:         string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
								}]
							}]
							ephemeralContainerStatuses?: [...{
								allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
								allocatedResourcesStatus?: [...{
									name?: string
									resources?: [...{
										health?:     string
										resourceID?: string
									}]
								}]
								containerID?: string
								image?:       string
								imageID?:     string
								lastState?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								name?:  string
								ready?: bool
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartCount?: int
								started?:      bool
								state?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								stopSignal?: string
								user?: linux?: {
									gid?: int
									supplementalGroups?: [...int]
									uid?: int
								}
								volumeMounts?: [...{
									mountPath?:         string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
								}]
							}]
							hostIP?: string
							hostIPs?: [...{
								ip?: string
							}]
							initContainerStatuses?: [...{
								allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
								allocatedResourcesStatus?: [...{
									name?: string
									resources?: [...{
										health?:     string
										resourceID?: string
									}]
								}]
								containerID?: string
								image?:       string
								imageID?:     string
								lastState?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								name?:  string
								ready?: bool
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartCount?: int
								started?:      bool
								state?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								stopSignal?: string
								user?: linux?: {
									gid?: int
									supplementalGroups?: [...int]
									uid?: int
								}
								volumeMounts?: [...{
									mountPath?:         string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
								}]
							}]
							message?:            string
							nominatedNodeName?:  string
							observedGeneration?: int
							phase?:              string
							podIP?:              string
							podIPs?: [...{
								ip?: string
							}]
							qosClass?: string
							reason?:   string
							resize?:   string
							resourceClaimStatuses?: [...{
								name?:              string
								resourceClaimName?: string
							}]
							startTime?: string
						}
					}
					replicas?: int
					resource?: {
						cpu?:              number
						ephemeralStorage?: string
						memory?:           string
					}
				}
				logConfiguration?: [string]: string
				mode?: "native" | "standalone"
				podTemplate?: {
					apiVersion?: string
					kind?:       string
					metadata?: {
						annotations?: [string]: string
						creationTimestamp?:          string
						deletionGracePeriodSeconds?: int
						deletionTimestamp?:          string
						finalizers?: [...string]
						generateName?: string
						generation?:   int
						labels?: [string]: string
						managedFields?: [...{
							apiVersion?: string
							fieldsType?: string
							fieldsV1?: {}
							manager?:     string
							operation?:   string
							subresource?: string
							time?:        string
						}]
						name?:      string
						namespace?: string
						ownerReferences?: [...{
							apiVersion?:         string
							blockOwnerDeletion?: bool
							controller?:         bool
							kind?:               string
							name?:               string
							uid?:                string
						}]
						resourceVersion?: string
						selfLink?:        string
						uid?:             string
					}
					spec?: {
						activeDeadlineSeconds?: int
						affinity?: {
							nodeAffinity?: {
								preferredDuringSchedulingIgnoredDuringExecution?: [...{
									preference?: {
										matchExpressions?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
										matchFields?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
									}
									weight?: int
								}]
								requiredDuringSchedulingIgnoredDuringExecution?: nodeSelectorTerms?: [...{
									matchExpressions?: [...{
										key?:      string
										operator?: string
										values?: [...string]
									}]
									matchFields?: [...{
										key?:      string
										operator?: string
										values?: [...string]
									}]
								}]
							}
							podAffinity?: {
								preferredDuringSchedulingIgnoredDuringExecution?: [...{
									podAffinityTerm?: {
										labelSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										matchLabelKeys?: [...string]
										mismatchLabelKeys?: [...string]
										namespaceSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										namespaces?: [...string]
										topologyKey?: string
									}
									weight?: int
								}]
								requiredDuringSchedulingIgnoredDuringExecution?: [...{
									labelSelector?: {
										matchExpressions?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
										matchLabels?: [string]: string
									}
									matchLabelKeys?: [...string]
									mismatchLabelKeys?: [...string]
									namespaceSelector?: {
										matchExpressions?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
										matchLabels?: [string]: string
									}
									namespaces?: [...string]
									topologyKey?: string
								}]
							}
							podAntiAffinity?: {
								preferredDuringSchedulingIgnoredDuringExecution?: [...{
									podAffinityTerm?: {
										labelSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										matchLabelKeys?: [...string]
										mismatchLabelKeys?: [...string]
										namespaceSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										namespaces?: [...string]
										topologyKey?: string
									}
									weight?: int
								}]
								requiredDuringSchedulingIgnoredDuringExecution?: [...{
									labelSelector?: {
										matchExpressions?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
										matchLabels?: [string]: string
									}
									matchLabelKeys?: [...string]
									mismatchLabelKeys?: [...string]
									namespaceSelector?: {
										matchExpressions?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
										matchLabels?: [string]: string
									}
									namespaces?: [...string]
									topologyKey?: string
								}]
							}
						}
						automountServiceAccountToken?: bool
						containers?: [...{
							args?: [...string]
							command?: [...string]
							env?: [...{
								name?:  string
								value?: string
								valueFrom?: {
									configMapKeyRef?: {
										key?:      string
										name?:     string
										optional?: bool
									}
									fieldRef?: {
										apiVersion?: string
										fieldPath?:  string
									}
									resourceFieldRef?: {
										containerName?: string
										divisor?: matchN(>=1, [int, string]) & (int | string)
										resource?: string
									}
									secretKeyRef?: {
										key?:      string
										name?:     string
										optional?: bool
									}
								}
							}]
							envFrom?: [...{
								configMapRef?: {
									name?:     string
									optional?: bool
								}
								prefix?: string
								secretRef?: {
									name?:     string
									optional?: bool
								}
							}]
							image?:           string
							imagePullPolicy?: string
							lifecycle?: {
								postStart?: {
									exec?: command?: [...string]
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									sleep?: seconds?: int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
								}
								preStop?: {
									exec?: command?: [...string]
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									sleep?: seconds?: int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
								}
								stopSignal?: string
							}
							livenessProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							name?: string
							ports?: [...{
								containerPort?: int
								hostIP?:        string
								hostPort?:      int
								name?:          string
								protocol?:      string
							}]
							readinessProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							resizePolicy?: [...{
								resourceName?:  string
								restartPolicy?: string
							}]
							resources?: {
								claims?: [...{
									name?:    string
									request?: string
								}]
								limits?: [string]: matchN(>=1, [int, string]) & (int | string)
								requests?: [string]: matchN(>=1, [int, string]) & (int | string)
							}
							restartPolicy?: string
							securityContext?: {
								allowPrivilegeEscalation?: bool
								appArmorProfile?: {
									localhostProfile?: string
									type?:             string
								}
								capabilities?: {
									add?: [...string]
									drop?: [...string]
								}
								privileged?:             bool
								procMount?:              string
								readOnlyRootFilesystem?: bool
								runAsGroup?:             int
								runAsNonRoot?:           bool
								runAsUser?:              int
								seLinuxOptions?: {
									level?: string
									role?:  string
									type?:  string
									user?:  string
								}
								seccompProfile?: {
									localhostProfile?: string
									type?:             string
								}
								windowsOptions?: {
									gmsaCredentialSpec?:     string
									gmsaCredentialSpecName?: string
									hostProcess?:            bool
									runAsUserName?:          string
								}
							}
							startupProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							stdin?:                    bool
							stdinOnce?:                bool
							terminationMessagePath?:   string
							terminationMessagePolicy?: string
							tty?:                      bool
							volumeDevices?: [...{
								devicePath?: string
								name?:       string
							}]
							volumeMounts?: [...{
								mountPath?:         string
								mountPropagation?:  string
								name?:              string
								readOnly?:          bool
								recursiveReadOnly?: string
								subPath?:           string
								subPathExpr?:       string
							}]
							workingDir?: string
						}]
						dnsConfig?: {
							nameservers?: [...string]
							options?: [...{
								name?:  string
								value?: string
							}]
							searches?: [...string]
						}
						dnsPolicy?:          string
						enableServiceLinks?: bool
						ephemeralContainers?: [...{
							args?: [...string]
							command?: [...string]
							env?: [...{
								name?:  string
								value?: string
								valueFrom?: {
									configMapKeyRef?: {
										key?:      string
										name?:     string
										optional?: bool
									}
									fieldRef?: {
										apiVersion?: string
										fieldPath?:  string
									}
									resourceFieldRef?: {
										containerName?: string
										divisor?: matchN(>=1, [int, string]) & (int | string)
										resource?: string
									}
									secretKeyRef?: {
										key?:      string
										name?:     string
										optional?: bool
									}
								}
							}]
							envFrom?: [...{
								configMapRef?: {
									name?:     string
									optional?: bool
								}
								prefix?: string
								secretRef?: {
									name?:     string
									optional?: bool
								}
							}]
							image?:           string
							imagePullPolicy?: string
							lifecycle?: {
								postStart?: {
									exec?: command?: [...string]
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									sleep?: seconds?: int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
								}
								preStop?: {
									exec?: command?: [...string]
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									sleep?: seconds?: int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
								}
								stopSignal?: string
							}
							livenessProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							name?: string
							ports?: [...{
								containerPort?: int
								hostIP?:        string
								hostPort?:      int
								name?:          string
								protocol?:      string
							}]
							readinessProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							resizePolicy?: [...{
								resourceName?:  string
								restartPolicy?: string
							}]
							resources?: {
								claims?: [...{
									name?:    string
									request?: string
								}]
								limits?: [string]: matchN(>=1, [int, string]) & (int | string)
								requests?: [string]: matchN(>=1, [int, string]) & (int | string)
							}
							restartPolicy?: string
							securityContext?: {
								allowPrivilegeEscalation?: bool
								appArmorProfile?: {
									localhostProfile?: string
									type?:             string
								}
								capabilities?: {
									add?: [...string]
									drop?: [...string]
								}
								privileged?:             bool
								procMount?:              string
								readOnlyRootFilesystem?: bool
								runAsGroup?:             int
								runAsNonRoot?:           bool
								runAsUser?:              int
								seLinuxOptions?: {
									level?: string
									role?:  string
									type?:  string
									user?:  string
								}
								seccompProfile?: {
									localhostProfile?: string
									type?:             string
								}
								windowsOptions?: {
									gmsaCredentialSpec?:     string
									gmsaCredentialSpecName?: string
									hostProcess?:            bool
									runAsUserName?:          string
								}
							}
							startupProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							stdin?:                    bool
							stdinOnce?:                bool
							targetContainerName?:      string
							terminationMessagePath?:   string
							terminationMessagePolicy?: string
							tty?:                      bool
							volumeDevices?: [...{
								devicePath?: string
								name?:       string
							}]
							volumeMounts?: [...{
								mountPath?:         string
								mountPropagation?:  string
								name?:              string
								readOnly?:          bool
								recursiveReadOnly?: string
								subPath?:           string
								subPathExpr?:       string
							}]
							workingDir?: string
						}]
						hostAliases?: [...{
							hostnames?: [...string]
							ip?: string
						}]
						hostIPC?:     bool
						hostNetwork?: bool
						hostPID?:     bool
						hostUsers?:   bool
						hostname?:    string
						imagePullSecrets?: [...{
							name?: string
						}]
						initContainers?: [...{
							args?: [...string]
							command?: [...string]
							env?: [...{
								name?:  string
								value?: string
								valueFrom?: {
									configMapKeyRef?: {
										key?:      string
										name?:     string
										optional?: bool
									}
									fieldRef?: {
										apiVersion?: string
										fieldPath?:  string
									}
									resourceFieldRef?: {
										containerName?: string
										divisor?: matchN(>=1, [int, string]) & (int | string)
										resource?: string
									}
									secretKeyRef?: {
										key?:      string
										name?:     string
										optional?: bool
									}
								}
							}]
							envFrom?: [...{
								configMapRef?: {
									name?:     string
									optional?: bool
								}
								prefix?: string
								secretRef?: {
									name?:     string
									optional?: bool
								}
							}]
							image?:           string
							imagePullPolicy?: string
							lifecycle?: {
								postStart?: {
									exec?: command?: [...string]
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									sleep?: seconds?: int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
								}
								preStop?: {
									exec?: command?: [...string]
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									sleep?: seconds?: int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
								}
								stopSignal?: string
							}
							livenessProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							name?: string
							ports?: [...{
								containerPort?: int
								hostIP?:        string
								hostPort?:      int
								name?:          string
								protocol?:      string
							}]
							readinessProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							resizePolicy?: [...{
								resourceName?:  string
								restartPolicy?: string
							}]
							resources?: {
								claims?: [...{
									name?:    string
									request?: string
								}]
								limits?: [string]: matchN(>=1, [int, string]) & (int | string)
								requests?: [string]: matchN(>=1, [int, string]) & (int | string)
							}
							restartPolicy?: string
							securityContext?: {
								allowPrivilegeEscalation?: bool
								appArmorProfile?: {
									localhostProfile?: string
									type?:             string
								}
								capabilities?: {
									add?: [...string]
									drop?: [...string]
								}
								privileged?:             bool
								procMount?:              string
								readOnlyRootFilesystem?: bool
								runAsGroup?:             int
								runAsNonRoot?:           bool
								runAsUser?:              int
								seLinuxOptions?: {
									level?: string
									role?:  string
									type?:  string
									user?:  string
								}
								seccompProfile?: {
									localhostProfile?: string
									type?:             string
								}
								windowsOptions?: {
									gmsaCredentialSpec?:     string
									gmsaCredentialSpecName?: string
									hostProcess?:            bool
									runAsUserName?:          string
								}
							}
							startupProbe?: {
								exec?: command?: [...string]
								failureThreshold?: int
								grpc?: {
									port?:    int
									service?: string
								}
								httpGet?: {
									host?: string
									httpHeaders?: [...{
										name?:  string
										value?: string
									}]
									path?: string
									port?: matchN(>=1, [int, string]) & (int | string)
									scheme?: string
								}
								initialDelaySeconds?: int
								periodSeconds?:       int
								successThreshold?:    int
								tcpSocket?: {
									host?: string
									port?: matchN(>=1, [int, string]) & (int | string)
								}
								terminationGracePeriodSeconds?: int
								timeoutSeconds?:                int
							}
							stdin?:                    bool
							stdinOnce?:                bool
							terminationMessagePath?:   string
							terminationMessagePolicy?: string
							tty?:                      bool
							volumeDevices?: [...{
								devicePath?: string
								name?:       string
							}]
							volumeMounts?: [...{
								mountPath?:         string
								mountPropagation?:  string
								name?:              string
								readOnly?:          bool
								recursiveReadOnly?: string
								subPath?:           string
								subPathExpr?:       string
							}]
							workingDir?: string
						}]
						nodeName?: string
						nodeSelector?: [string]: string
						os?: name?:              string
						overhead?: [string]: matchN(>=1, [int, string]) & (int | string)
						preemptionPolicy?:  string
						priority?:          int
						priorityClassName?: string
						readinessGates?: [...{
							conditionType?: string
						}]
						resourceClaims?: [...{
							name?:                      string
							resourceClaimName?:         string
							resourceClaimTemplateName?: string
						}]
						resources?: {
							claims?: [...{
								name?:    string
								request?: string
							}]
							limits?: [string]: matchN(>=1, [int, string]) & (int | string)
							requests?: [string]: matchN(>=1, [int, string]) & (int | string)
						}
						restartPolicy?:    string
						runtimeClassName?: string
						schedulerName?:    string
						schedulingGates?: [...{
							name?: string
						}]
						securityContext?: {
							appArmorProfile?: {
								localhostProfile?: string
								type?:             string
							}
							fsGroup?:             int
							fsGroupChangePolicy?: string
							runAsGroup?:          int
							runAsNonRoot?:        bool
							runAsUser?:           int
							seLinuxChangePolicy?: string
							seLinuxOptions?: {
								level?: string
								role?:  string
								type?:  string
								user?:  string
							}
							seccompProfile?: {
								localhostProfile?: string
								type?:             string
							}
							supplementalGroups?: [...int]
							supplementalGroupsPolicy?: string
							sysctls?: [...{
								name?:  string
								value?: string
							}]
							windowsOptions?: {
								gmsaCredentialSpec?:     string
								gmsaCredentialSpecName?: string
								hostProcess?:            bool
								runAsUserName?:          string
							}
						}
						serviceAccount?:                string
						serviceAccountName?:            string
						setHostnameAsFQDN?:             bool
						shareProcessNamespace?:         bool
						subdomain?:                     string
						terminationGracePeriodSeconds?: int
						tolerations?: [...{
							effect?:            string
							key?:               string
							operator?:          string
							tolerationSeconds?: int
							value?:             string
						}]
						topologySpreadConstraints?: [...{
							labelSelector?: {
								matchExpressions?: [...{
									key?:      string
									operator?: string
									values?: [...string]
								}]
								matchLabels?: [string]: string
							}
							matchLabelKeys?: [...string]
							maxSkew?:            int
							minDomains?:         int
							nodeAffinityPolicy?: string
							nodeTaintsPolicy?:   string
							topologyKey?:        string
							whenUnsatisfiable?:  string
						}]
						volumes?: [...{
							awsElasticBlockStore?: {
								fsType?:    string
								partition?: int
								readOnly?:  bool
								volumeID?:  string
							}
							azureDisk?: {
								cachingMode?: string
								diskName?:    string
								diskURI?:     string
								fsType?:      string
								kind?:        string
								readOnly?:    bool
							}
							azureFile?: {
								readOnly?:   bool
								secretName?: string
								shareName?:  string
							}
							cephfs?: {
								monitors?: [...string]
								path?:       string
								readOnly?:   bool
								secretFile?: string
								secretRef?: name?: string
								user?: string
							}
							cinder?: {
								fsType?:   string
								readOnly?: bool
								secretRef?: name?: string
								volumeID?: string
							}
							configMap?: {
								defaultMode?: int
								items?: [...{
									key?:  string
									mode?: int
									path?: string
								}]
								name?:     string
								optional?: bool
							}
							csi?: {
								driver?: string
								fsType?: string
								nodePublishSecretRef?: name?: string
								readOnly?: bool
								volumeAttributes?: [string]: string
							}
							downwardAPI?: {
								defaultMode?: int
								items?: [...{
									fieldRef?: {
										apiVersion?: string
										fieldPath?:  string
									}
									mode?: int
									path?: string
									resourceFieldRef?: {
										containerName?: string
										divisor?: matchN(>=1, [int, string]) & (int | string)
										resource?: string
									}
								}]
							}
							emptyDir?: {
								medium?: string
								sizeLimit?: matchN(>=1, [int, string]) & (int | string)
							}
							ephemeral?: volumeClaimTemplate?: {
								metadata?: {
									annotations?: [string]: string
									creationTimestamp?:          string
									deletionGracePeriodSeconds?: int
									deletionTimestamp?:          string
									finalizers?: [...string]
									generateName?: string
									generation?:   int
									labels?: [string]: string
									managedFields?: [...{
										apiVersion?: string
										fieldsType?: string
										fieldsV1?: {}
										manager?:     string
										operation?:   string
										subresource?: string
										time?:        string
									}]
									name?:      string
									namespace?: string
									ownerReferences?: [...{
										apiVersion?:         string
										blockOwnerDeletion?: bool
										controller?:         bool
										kind?:               string
										name?:               string
										uid?:                string
									}]
									resourceVersion?: string
									selfLink?:        string
									uid?:             string
								}
								spec?: {
									accessModes?: [...string]
									dataSource?: {
										apiGroup?: string
										kind?:     string
										name?:     string
									}
									dataSourceRef?: {
										apiGroup?:  string
										kind?:      string
										name?:      string
										namespace?: string
									}
									resources?: {
										limits?: [string]: matchN(>=1, [int, string]) & (int | string)
										requests?: [string]: matchN(>=1, [int, string]) & (int | string)
									}
									selector?: {
										matchExpressions?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
										matchLabels?: [string]: string
									}
									storageClassName?:          string
									volumeAttributesClassName?: string
									volumeMode?:                string
									volumeName?:                string
								}
							}
							fc?: {
								fsType?:   string
								lun?:      int
								readOnly?: bool
								targetWWNs?: [...string]
								wwids?: [...string]
							}
							flexVolume?: {
								driver?: string
								fsType?: string
								options?: [string]: string
								readOnly?: bool
								secretRef?: name?: string
							}
							flocker?: {
								datasetName?: string
								datasetUUID?: string
							}
							gcePersistentDisk?: {
								fsType?:    string
								partition?: int
								pdName?:    string
								readOnly?:  bool
							}
							gitRepo?: {
								directory?:  string
								repository?: string
								revision?:   string
							}
							glusterfs?: {
								endpoints?: string
								path?:      string
								readOnly?:  bool
							}
							hostPath?: {
								path?: string
								type?: string
							}
							image?: {
								pullPolicy?: string
								reference?:  string
							}
							iscsi?: {
								chapAuthDiscovery?: bool
								chapAuthSession?:   bool
								fsType?:            string
								initiatorName?:     string
								iqn?:               string
								iscsiInterface?:    string
								lun?:               int
								portals?: [...string]
								readOnly?: bool
								secretRef?: name?: string
								targetPortal?: string
							}
							name?: string
							nfs?: {
								path?:     string
								readOnly?: bool
								server?:   string
							}
							persistentVolumeClaim?: {
								claimName?: string
								readOnly?:  bool
							}
							photonPersistentDisk?: {
								fsType?: string
								pdID?:   string
							}
							portworxVolume?: {
								fsType?:   string
								readOnly?: bool
								volumeID?: string
							}
							projected?: {
								defaultMode?: int
								sources?: [...{
									clusterTrustBundle?: {
										labelSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										name?:       string
										optional?:   bool
										path?:       string
										signerName?: string
									}
									configMap?: {
										items?: [...{
											key?:  string
											mode?: int
											path?: string
										}]
										name?:     string
										optional?: bool
									}
									downwardAPI?: items?: [...{
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										mode?: int
										path?: string
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
									}]
									secret?: {
										items?: [...{
											key?:  string
											mode?: int
											path?: string
										}]
										name?:     string
										optional?: bool
									}
									serviceAccountToken?: {
										audience?:          string
										expirationSeconds?: int
										path?:              string
									}
								}]
							}
							quobyte?: {
								group?:    string
								readOnly?: bool
								registry?: string
								tenant?:   string
								user?:     string
								volume?:   string
							}
							rbd?: {
								fsType?:  string
								image?:   string
								keyring?: string
								monitors?: [...string]
								pool?:     string
								readOnly?: bool
								secretRef?: name?: string
								user?: string
							}
							scaleIO?: {
								fsType?:           string
								gateway?:          string
								protectionDomain?: string
								readOnly?:         bool
								secretRef?: name?: string
								sslEnabled?:  bool
								storageMode?: string
								storagePool?: string
								system?:      string
								volumeName?:  string
							}
							secret?: {
								defaultMode?: int
								items?: [...{
									key?:  string
									mode?: int
									path?: string
								}]
								optional?:   bool
								secretName?: string
							}
							storageos?: {
								fsType?:   string
								readOnly?: bool
								secretRef?: name?: string
								volumeName?:      string
								volumeNamespace?: string
							}
							vsphereVolume?: {
								fsType?:            string
								storagePolicyID?:   string
								storagePolicyName?: string
								volumePath?:        string
							}
						}]
					}
					status?: {
						conditions?: [...{
							lastProbeTime?:      string
							lastTransitionTime?: string
							message?:            string
							observedGeneration?: int
							reason?:             string
							status?:             string
							type?:               string
						}]
						containerStatuses?: [...{
							allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
							allocatedResourcesStatus?: [...{
								name?: string
								resources?: [...{
									health?:     string
									resourceID?: string
								}]
							}]
							containerID?: string
							image?:       string
							imageID?:     string
							lastState?: {
								running?: startedAt?: string
								terminated?: {
									containerID?: string
									exitCode?:    int
									finishedAt?:  string
									message?:     string
									reason?:      string
									signal?:      int
									startedAt?:   string
								}
								waiting?: {
									message?: string
									reason?:  string
								}
							}
							name?:  string
							ready?: bool
							resources?: {
								claims?: [...{
									name?:    string
									request?: string
								}]
								limits?: [string]: matchN(>=1, [int, string]) & (int | string)
								requests?: [string]: matchN(>=1, [int, string]) & (int | string)
							}
							restartCount?: int
							started?:      bool
							state?: {
								running?: startedAt?: string
								terminated?: {
									containerID?: string
									exitCode?:    int
									finishedAt?:  string
									message?:     string
									reason?:      string
									signal?:      int
									startedAt?:   string
								}
								waiting?: {
									message?: string
									reason?:  string
								}
							}
							stopSignal?: string
							user?: linux?: {
								gid?: int
								supplementalGroups?: [...int]
								uid?: int
							}
							volumeMounts?: [...{
								mountPath?:         string
								name?:              string
								readOnly?:          bool
								recursiveReadOnly?: string
							}]
						}]
						ephemeralContainerStatuses?: [...{
							allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
							allocatedResourcesStatus?: [...{
								name?: string
								resources?: [...{
									health?:     string
									resourceID?: string
								}]
							}]
							containerID?: string
							image?:       string
							imageID?:     string
							lastState?: {
								running?: startedAt?: string
								terminated?: {
									containerID?: string
									exitCode?:    int
									finishedAt?:  string
									message?:     string
									reason?:      string
									signal?:      int
									startedAt?:   string
								}
								waiting?: {
									message?: string
									reason?:  string
								}
							}
							name?:  string
							ready?: bool
							resources?: {
								claims?: [...{
									name?:    string
									request?: string
								}]
								limits?: [string]: matchN(>=1, [int, string]) & (int | string)
								requests?: [string]: matchN(>=1, [int, string]) & (int | string)
							}
							restartCount?: int
							started?:      bool
							state?: {
								running?: startedAt?: string
								terminated?: {
									containerID?: string
									exitCode?:    int
									finishedAt?:  string
									message?:     string
									reason?:      string
									signal?:      int
									startedAt?:   string
								}
								waiting?: {
									message?: string
									reason?:  string
								}
							}
							stopSignal?: string
							user?: linux?: {
								gid?: int
								supplementalGroups?: [...int]
								uid?: int
							}
							volumeMounts?: [...{
								mountPath?:         string
								name?:              string
								readOnly?:          bool
								recursiveReadOnly?: string
							}]
						}]
						hostIP?: string
						hostIPs?: [...{
							ip?: string
						}]
						initContainerStatuses?: [...{
							allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
							allocatedResourcesStatus?: [...{
								name?: string
								resources?: [...{
									health?:     string
									resourceID?: string
								}]
							}]
							containerID?: string
							image?:       string
							imageID?:     string
							lastState?: {
								running?: startedAt?: string
								terminated?: {
									containerID?: string
									exitCode?:    int
									finishedAt?:  string
									message?:     string
									reason?:      string
									signal?:      int
									startedAt?:   string
								}
								waiting?: {
									message?: string
									reason?:  string
								}
							}
							name?:  string
							ready?: bool
							resources?: {
								claims?: [...{
									name?:    string
									request?: string
								}]
								limits?: [string]: matchN(>=1, [int, string]) & (int | string)
								requests?: [string]: matchN(>=1, [int, string]) & (int | string)
							}
							restartCount?: int
							started?:      bool
							state?: {
								running?: startedAt?: string
								terminated?: {
									containerID?: string
									exitCode?:    int
									finishedAt?:  string
									message?:     string
									reason?:      string
									signal?:      int
									startedAt?:   string
								}
								waiting?: {
									message?: string
									reason?:  string
								}
							}
							stopSignal?: string
							user?: linux?: {
								gid?: int
								supplementalGroups?: [...int]
								uid?: int
							}
							volumeMounts?: [...{
								mountPath?:         string
								name?:              string
								readOnly?:          bool
								recursiveReadOnly?: string
							}]
						}]
						message?:            string
						nominatedNodeName?:  string
						observedGeneration?: int
						phase?:              string
						podIP?:              string
						podIPs?: [...{
							ip?: string
						}]
						qosClass?: string
						reason?:   string
						resize?:   string
						resourceClaimStatuses?: [...{
							name?:              string
							resourceClaimName?: string
						}]
						startTime?: string
					}
				}
				restartNonce?:   int
				serviceAccount?: string
				taskManager?: {
					podTemplate?: {
						apiVersion?: string
						kind?:       string
						metadata?: {
							annotations?: [string]: string
							creationTimestamp?:          string
							deletionGracePeriodSeconds?: int
							deletionTimestamp?:          string
							finalizers?: [...string]
							generateName?: string
							generation?:   int
							labels?: [string]: string
							managedFields?: [...{
								apiVersion?: string
								fieldsType?: string
								fieldsV1?: {}
								manager?:     string
								operation?:   string
								subresource?: string
								time?:        string
							}]
							name?:      string
							namespace?: string
							ownerReferences?: [...{
								apiVersion?:         string
								blockOwnerDeletion?: bool
								controller?:         bool
								kind?:               string
								name?:               string
								uid?:                string
							}]
							resourceVersion?: string
							selfLink?:        string
							uid?:             string
						}
						spec?: {
							activeDeadlineSeconds?: int
							affinity?: {
								nodeAffinity?: {
									preferredDuringSchedulingIgnoredDuringExecution?: [...{
										preference?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchFields?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
										}
										weight?: int
									}]
									requiredDuringSchedulingIgnoredDuringExecution?: nodeSelectorTerms?: [...{
										matchExpressions?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
										matchFields?: [...{
											key?:      string
											operator?: string
											values?: [...string]
										}]
									}]
								}
								podAffinity?: {
									preferredDuringSchedulingIgnoredDuringExecution?: [...{
										podAffinityTerm?: {
											labelSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											matchLabelKeys?: [...string]
											mismatchLabelKeys?: [...string]
											namespaceSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											namespaces?: [...string]
											topologyKey?: string
										}
										weight?: int
									}]
									requiredDuringSchedulingIgnoredDuringExecution?: [...{
										labelSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										matchLabelKeys?: [...string]
										mismatchLabelKeys?: [...string]
										namespaceSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										namespaces?: [...string]
										topologyKey?: string
									}]
								}
								podAntiAffinity?: {
									preferredDuringSchedulingIgnoredDuringExecution?: [...{
										podAffinityTerm?: {
											labelSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											matchLabelKeys?: [...string]
											mismatchLabelKeys?: [...string]
											namespaceSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											namespaces?: [...string]
											topologyKey?: string
										}
										weight?: int
									}]
									requiredDuringSchedulingIgnoredDuringExecution?: [...{
										labelSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										matchLabelKeys?: [...string]
										mismatchLabelKeys?: [...string]
										namespaceSelector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										namespaces?: [...string]
										topologyKey?: string
									}]
								}
							}
							automountServiceAccountToken?: bool
							containers?: [...{
								args?: [...string]
								command?: [...string]
								env?: [...{
									name?:  string
									value?: string
									valueFrom?: {
										configMapKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
										secretKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
									}
								}]
								envFrom?: [...{
									configMapRef?: {
										name?:     string
										optional?: bool
									}
									prefix?: string
									secretRef?: {
										name?:     string
										optional?: bool
									}
								}]
								image?:           string
								imagePullPolicy?: string
								lifecycle?: {
									postStart?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									preStop?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									stopSignal?: string
								}
								livenessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								name?: string
								ports?: [...{
									containerPort?: int
									hostIP?:        string
									hostPort?:      int
									name?:          string
									protocol?:      string
								}]
								readinessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								resizePolicy?: [...{
									resourceName?:  string
									restartPolicy?: string
								}]
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartPolicy?: string
								securityContext?: {
									allowPrivilegeEscalation?: bool
									appArmorProfile?: {
										localhostProfile?: string
										type?:             string
									}
									capabilities?: {
										add?: [...string]
										drop?: [...string]
									}
									privileged?:             bool
									procMount?:              string
									readOnlyRootFilesystem?: bool
									runAsGroup?:             int
									runAsNonRoot?:           bool
									runAsUser?:              int
									seLinuxOptions?: {
										level?: string
										role?:  string
										type?:  string
										user?:  string
									}
									seccompProfile?: {
										localhostProfile?: string
										type?:             string
									}
									windowsOptions?: {
										gmsaCredentialSpec?:     string
										gmsaCredentialSpecName?: string
										hostProcess?:            bool
										runAsUserName?:          string
									}
								}
								startupProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								stdin?:                    bool
								stdinOnce?:                bool
								terminationMessagePath?:   string
								terminationMessagePolicy?: string
								tty?:                      bool
								volumeDevices?: [...{
									devicePath?: string
									name?:       string
								}]
								volumeMounts?: [...{
									mountPath?:         string
									mountPropagation?:  string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
									subPath?:           string
									subPathExpr?:       string
								}]
								workingDir?: string
							}]
							dnsConfig?: {
								nameservers?: [...string]
								options?: [...{
									name?:  string
									value?: string
								}]
								searches?: [...string]
							}
							dnsPolicy?:          string
							enableServiceLinks?: bool
							ephemeralContainers?: [...{
								args?: [...string]
								command?: [...string]
								env?: [...{
									name?:  string
									value?: string
									valueFrom?: {
										configMapKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
										secretKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
									}
								}]
								envFrom?: [...{
									configMapRef?: {
										name?:     string
										optional?: bool
									}
									prefix?: string
									secretRef?: {
										name?:     string
										optional?: bool
									}
								}]
								image?:           string
								imagePullPolicy?: string
								lifecycle?: {
									postStart?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									preStop?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									stopSignal?: string
								}
								livenessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								name?: string
								ports?: [...{
									containerPort?: int
									hostIP?:        string
									hostPort?:      int
									name?:          string
									protocol?:      string
								}]
								readinessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								resizePolicy?: [...{
									resourceName?:  string
									restartPolicy?: string
								}]
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartPolicy?: string
								securityContext?: {
									allowPrivilegeEscalation?: bool
									appArmorProfile?: {
										localhostProfile?: string
										type?:             string
									}
									capabilities?: {
										add?: [...string]
										drop?: [...string]
									}
									privileged?:             bool
									procMount?:              string
									readOnlyRootFilesystem?: bool
									runAsGroup?:             int
									runAsNonRoot?:           bool
									runAsUser?:              int
									seLinuxOptions?: {
										level?: string
										role?:  string
										type?:  string
										user?:  string
									}
									seccompProfile?: {
										localhostProfile?: string
										type?:             string
									}
									windowsOptions?: {
										gmsaCredentialSpec?:     string
										gmsaCredentialSpecName?: string
										hostProcess?:            bool
										runAsUserName?:          string
									}
								}
								startupProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								stdin?:                    bool
								stdinOnce?:                bool
								targetContainerName?:      string
								terminationMessagePath?:   string
								terminationMessagePolicy?: string
								tty?:                      bool
								volumeDevices?: [...{
									devicePath?: string
									name?:       string
								}]
								volumeMounts?: [...{
									mountPath?:         string
									mountPropagation?:  string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
									subPath?:           string
									subPathExpr?:       string
								}]
								workingDir?: string
							}]
							hostAliases?: [...{
								hostnames?: [...string]
								ip?: string
							}]
							hostIPC?:     bool
							hostNetwork?: bool
							hostPID?:     bool
							hostUsers?:   bool
							hostname?:    string
							imagePullSecrets?: [...{
								name?: string
							}]
							initContainers?: [...{
								args?: [...string]
								command?: [...string]
								env?: [...{
									name?:  string
									value?: string
									valueFrom?: {
										configMapKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
										secretKeyRef?: {
											key?:      string
											name?:     string
											optional?: bool
										}
									}
								}]
								envFrom?: [...{
									configMapRef?: {
										name?:     string
										optional?: bool
									}
									prefix?: string
									secretRef?: {
										name?:     string
										optional?: bool
									}
								}]
								image?:           string
								imagePullPolicy?: string
								lifecycle?: {
									postStart?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									preStop?: {
										exec?: command?: [...string]
										httpGet?: {
											host?: string
											httpHeaders?: [...{
												name?:  string
												value?: string
											}]
											path?: string
											port?: matchN(>=1, [int, string]) & (int | string)
											scheme?: string
										}
										sleep?: seconds?: int
										tcpSocket?: {
											host?: string
											port?: matchN(>=1, [int, string]) & (int | string)
										}
									}
									stopSignal?: string
								}
								livenessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								name?: string
								ports?: [...{
									containerPort?: int
									hostIP?:        string
									hostPort?:      int
									name?:          string
									protocol?:      string
								}]
								readinessProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								resizePolicy?: [...{
									resourceName?:  string
									restartPolicy?: string
								}]
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartPolicy?: string
								securityContext?: {
									allowPrivilegeEscalation?: bool
									appArmorProfile?: {
										localhostProfile?: string
										type?:             string
									}
									capabilities?: {
										add?: [...string]
										drop?: [...string]
									}
									privileged?:             bool
									procMount?:              string
									readOnlyRootFilesystem?: bool
									runAsGroup?:             int
									runAsNonRoot?:           bool
									runAsUser?:              int
									seLinuxOptions?: {
										level?: string
										role?:  string
										type?:  string
										user?:  string
									}
									seccompProfile?: {
										localhostProfile?: string
										type?:             string
									}
									windowsOptions?: {
										gmsaCredentialSpec?:     string
										gmsaCredentialSpecName?: string
										hostProcess?:            bool
										runAsUserName?:          string
									}
								}
								startupProbe?: {
									exec?: command?: [...string]
									failureThreshold?: int
									grpc?: {
										port?:    int
										service?: string
									}
									httpGet?: {
										host?: string
										httpHeaders?: [...{
											name?:  string
											value?: string
										}]
										path?: string
										port?: matchN(>=1, [int, string]) & (int | string)
										scheme?: string
									}
									initialDelaySeconds?: int
									periodSeconds?:       int
									successThreshold?:    int
									tcpSocket?: {
										host?: string
										port?: matchN(>=1, [int, string]) & (int | string)
									}
									terminationGracePeriodSeconds?: int
									timeoutSeconds?:                int
								}
								stdin?:                    bool
								stdinOnce?:                bool
								terminationMessagePath?:   string
								terminationMessagePolicy?: string
								tty?:                      bool
								volumeDevices?: [...{
									devicePath?: string
									name?:       string
								}]
								volumeMounts?: [...{
									mountPath?:         string
									mountPropagation?:  string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
									subPath?:           string
									subPathExpr?:       string
								}]
								workingDir?: string
							}]
							nodeName?: string
							nodeSelector?: [string]: string
							os?: name?:              string
							overhead?: [string]: matchN(>=1, [int, string]) & (int | string)
							preemptionPolicy?:  string
							priority?:          int
							priorityClassName?: string
							readinessGates?: [...{
								conditionType?: string
							}]
							resourceClaims?: [...{
								name?:                      string
								resourceClaimName?:         string
								resourceClaimTemplateName?: string
							}]
							resources?: {
								claims?: [...{
									name?:    string
									request?: string
								}]
								limits?: [string]: matchN(>=1, [int, string]) & (int | string)
								requests?: [string]: matchN(>=1, [int, string]) & (int | string)
							}
							restartPolicy?:    string
							runtimeClassName?: string
							schedulerName?:    string
							schedulingGates?: [...{
								name?: string
							}]
							securityContext?: {
								appArmorProfile?: {
									localhostProfile?: string
									type?:             string
								}
								fsGroup?:             int
								fsGroupChangePolicy?: string
								runAsGroup?:          int
								runAsNonRoot?:        bool
								runAsUser?:           int
								seLinuxChangePolicy?: string
								seLinuxOptions?: {
									level?: string
									role?:  string
									type?:  string
									user?:  string
								}
								seccompProfile?: {
									localhostProfile?: string
									type?:             string
								}
								supplementalGroups?: [...int]
								supplementalGroupsPolicy?: string
								sysctls?: [...{
									name?:  string
									value?: string
								}]
								windowsOptions?: {
									gmsaCredentialSpec?:     string
									gmsaCredentialSpecName?: string
									hostProcess?:            bool
									runAsUserName?:          string
								}
							}
							serviceAccount?:                string
							serviceAccountName?:            string
							setHostnameAsFQDN?:             bool
							shareProcessNamespace?:         bool
							subdomain?:                     string
							terminationGracePeriodSeconds?: int
							tolerations?: [...{
								effect?:            string
								key?:               string
								operator?:          string
								tolerationSeconds?: int
								value?:             string
							}]
							topologySpreadConstraints?: [...{
								labelSelector?: {
									matchExpressions?: [...{
										key?:      string
										operator?: string
										values?: [...string]
									}]
									matchLabels?: [string]: string
								}
								matchLabelKeys?: [...string]
								maxSkew?:            int
								minDomains?:         int
								nodeAffinityPolicy?: string
								nodeTaintsPolicy?:   string
								topologyKey?:        string
								whenUnsatisfiable?:  string
							}]
							volumes?: [...{
								awsElasticBlockStore?: {
									fsType?:    string
									partition?: int
									readOnly?:  bool
									volumeID?:  string
								}
								azureDisk?: {
									cachingMode?: string
									diskName?:    string
									diskURI?:     string
									fsType?:      string
									kind?:        string
									readOnly?:    bool
								}
								azureFile?: {
									readOnly?:   bool
									secretName?: string
									shareName?:  string
								}
								cephfs?: {
									monitors?: [...string]
									path?:       string
									readOnly?:   bool
									secretFile?: string
									secretRef?: name?: string
									user?: string
								}
								cinder?: {
									fsType?:   string
									readOnly?: bool
									secretRef?: name?: string
									volumeID?: string
								}
								configMap?: {
									defaultMode?: int
									items?: [...{
										key?:  string
										mode?: int
										path?: string
									}]
									name?:     string
									optional?: bool
								}
								csi?: {
									driver?: string
									fsType?: string
									nodePublishSecretRef?: name?: string
									readOnly?: bool
									volumeAttributes?: [string]: string
								}
								downwardAPI?: {
									defaultMode?: int
									items?: [...{
										fieldRef?: {
											apiVersion?: string
											fieldPath?:  string
										}
										mode?: int
										path?: string
										resourceFieldRef?: {
											containerName?: string
											divisor?: matchN(>=1, [int, string]) & (int | string)
											resource?: string
										}
									}]
								}
								emptyDir?: {
									medium?: string
									sizeLimit?: matchN(>=1, [int, string]) & (int | string)
								}
								ephemeral?: volumeClaimTemplate?: {
									metadata?: {
										annotations?: [string]: string
										creationTimestamp?:          string
										deletionGracePeriodSeconds?: int
										deletionTimestamp?:          string
										finalizers?: [...string]
										generateName?: string
										generation?:   int
										labels?: [string]: string
										managedFields?: [...{
											apiVersion?: string
											fieldsType?: string
											fieldsV1?: {}
											manager?:     string
											operation?:   string
											subresource?: string
											time?:        string
										}]
										name?:      string
										namespace?: string
										ownerReferences?: [...{
											apiVersion?:         string
											blockOwnerDeletion?: bool
											controller?:         bool
											kind?:               string
											name?:               string
											uid?:                string
										}]
										resourceVersion?: string
										selfLink?:        string
										uid?:             string
									}
									spec?: {
										accessModes?: [...string]
										dataSource?: {
											apiGroup?: string
											kind?:     string
											name?:     string
										}
										dataSourceRef?: {
											apiGroup?:  string
											kind?:      string
											name?:      string
											namespace?: string
										}
										resources?: {
											limits?: [string]: matchN(>=1, [int, string]) & (int | string)
											requests?: [string]: matchN(>=1, [int, string]) & (int | string)
										}
										selector?: {
											matchExpressions?: [...{
												key?:      string
												operator?: string
												values?: [...string]
											}]
											matchLabels?: [string]: string
										}
										storageClassName?:          string
										volumeAttributesClassName?: string
										volumeMode?:                string
										volumeName?:                string
									}
								}
								fc?: {
									fsType?:   string
									lun?:      int
									readOnly?: bool
									targetWWNs?: [...string]
									wwids?: [...string]
								}
								flexVolume?: {
									driver?: string
									fsType?: string
									options?: [string]: string
									readOnly?: bool
									secretRef?: name?: string
								}
								flocker?: {
									datasetName?: string
									datasetUUID?: string
								}
								gcePersistentDisk?: {
									fsType?:    string
									partition?: int
									pdName?:    string
									readOnly?:  bool
								}
								gitRepo?: {
									directory?:  string
									repository?: string
									revision?:   string
								}
								glusterfs?: {
									endpoints?: string
									path?:      string
									readOnly?:  bool
								}
								hostPath?: {
									path?: string
									type?: string
								}
								image?: {
									pullPolicy?: string
									reference?:  string
								}
								iscsi?: {
									chapAuthDiscovery?: bool
									chapAuthSession?:   bool
									fsType?:            string
									initiatorName?:     string
									iqn?:               string
									iscsiInterface?:    string
									lun?:               int
									portals?: [...string]
									readOnly?: bool
									secretRef?: name?: string
									targetPortal?: string
								}
								name?: string
								nfs?: {
									path?:     string
									readOnly?: bool
									server?:   string
								}
								persistentVolumeClaim?: {
									claimName?: string
									readOnly?:  bool
								}
								photonPersistentDisk?: {
									fsType?: string
									pdID?:   string
								}
								portworxVolume?: {
									fsType?:   string
									readOnly?: bool
									volumeID?: string
								}
								projected?: {
									defaultMode?: int
									sources?: [...{
										clusterTrustBundle?: {
											labelSelector?: {
												matchExpressions?: [...{
													key?:      string
													operator?: string
													values?: [...string]
												}]
												matchLabels?: [string]: string
											}
											name?:       string
											optional?:   bool
											path?:       string
											signerName?: string
										}
										configMap?: {
											items?: [...{
												key?:  string
												mode?: int
												path?: string
											}]
											name?:     string
											optional?: bool
										}
										downwardAPI?: items?: [...{
											fieldRef?: {
												apiVersion?: string
												fieldPath?:  string
											}
											mode?: int
											path?: string
											resourceFieldRef?: {
												containerName?: string
												divisor?: matchN(>=1, [int, string]) & (int | string)
												resource?: string
											}
										}]
										secret?: {
											items?: [...{
												key?:  string
												mode?: int
												path?: string
											}]
											name?:     string
											optional?: bool
										}
										serviceAccountToken?: {
											audience?:          string
											expirationSeconds?: int
											path?:              string
										}
									}]
								}
								quobyte?: {
									group?:    string
									readOnly?: bool
									registry?: string
									tenant?:   string
									user?:     string
									volume?:   string
								}
								rbd?: {
									fsType?:  string
									image?:   string
									keyring?: string
									monitors?: [...string]
									pool?:     string
									readOnly?: bool
									secretRef?: name?: string
									user?: string
								}
								scaleIO?: {
									fsType?:           string
									gateway?:          string
									protectionDomain?: string
									readOnly?:         bool
									secretRef?: name?: string
									sslEnabled?:  bool
									storageMode?: string
									storagePool?: string
									system?:      string
									volumeName?:  string
								}
								secret?: {
									defaultMode?: int
									items?: [...{
										key?:  string
										mode?: int
										path?: string
									}]
									optional?:   bool
									secretName?: string
								}
								storageos?: {
									fsType?:   string
									readOnly?: bool
									secretRef?: name?: string
									volumeName?:      string
									volumeNamespace?: string
								}
								vsphereVolume?: {
									fsType?:            string
									storagePolicyID?:   string
									storagePolicyName?: string
									volumePath?:        string
								}
							}]
						}
						status?: {
							conditions?: [...{
								lastProbeTime?:      string
								lastTransitionTime?: string
								message?:            string
								observedGeneration?: int
								reason?:             string
								status?:             string
								type?:               string
							}]
							containerStatuses?: [...{
								allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
								allocatedResourcesStatus?: [...{
									name?: string
									resources?: [...{
										health?:     string
										resourceID?: string
									}]
								}]
								containerID?: string
								image?:       string
								imageID?:     string
								lastState?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								name?:  string
								ready?: bool
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartCount?: int
								started?:      bool
								state?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								stopSignal?: string
								user?: linux?: {
									gid?: int
									supplementalGroups?: [...int]
									uid?: int
								}
								volumeMounts?: [...{
									mountPath?:         string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
								}]
							}]
							ephemeralContainerStatuses?: [...{
								allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
								allocatedResourcesStatus?: [...{
									name?: string
									resources?: [...{
										health?:     string
										resourceID?: string
									}]
								}]
								containerID?: string
								image?:       string
								imageID?:     string
								lastState?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								name?:  string
								ready?: bool
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartCount?: int
								started?:      bool
								state?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								stopSignal?: string
								user?: linux?: {
									gid?: int
									supplementalGroups?: [...int]
									uid?: int
								}
								volumeMounts?: [...{
									mountPath?:         string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
								}]
							}]
							hostIP?: string
							hostIPs?: [...{
								ip?: string
							}]
							initContainerStatuses?: [...{
								allocatedResources?: [string]: matchN(>=1, [int, string]) & (int | string)
								allocatedResourcesStatus?: [...{
									name?: string
									resources?: [...{
										health?:     string
										resourceID?: string
									}]
								}]
								containerID?: string
								image?:       string
								imageID?:     string
								lastState?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								name?:  string
								ready?: bool
								resources?: {
									claims?: [...{
										name?:    string
										request?: string
									}]
									limits?: [string]: matchN(>=1, [int, string]) & (int | string)
									requests?: [string]: matchN(>=1, [int, string]) & (int | string)
								}
								restartCount?: int
								started?:      bool
								state?: {
									running?: startedAt?: string
									terminated?: {
										containerID?: string
										exitCode?:    int
										finishedAt?:  string
										message?:     string
										reason?:      string
										signal?:      int
										startedAt?:   string
									}
									waiting?: {
										message?: string
										reason?:  string
									}
								}
								stopSignal?: string
								user?: linux?: {
									gid?: int
									supplementalGroups?: [...int]
									uid?: int
								}
								volumeMounts?: [...{
									mountPath?:         string
									name?:              string
									readOnly?:          bool
									recursiveReadOnly?: string
								}]
							}]
							message?:            string
							nominatedNodeName?:  string
							observedGeneration?: int
							phase?:              string
							podIP?:              string
							podIPs?: [...{
								ip?: string
							}]
							qosClass?: string
							reason?:   string
							resize?:   string
							resourceClaimStatuses?: [...{
								name?:              string
								resourceClaimName?: string
							}]
							startTime?: string
						}
					}
					replicas?: int
					resource?: {
						cpu?:              number
						ephemeralStorage?: string
						memory?:           string
					}
				}
			}
		}
	}
	status?: {
		abortTimestamp?:           string
		blueGreenState?:           "ACTIVE_BLUE" | "ACTIVE_GREEN" | "INITIALIZING_BLUE" | "SAVEPOINTING_BLUE" | "SAVEPOINTING_GREEN" | "TRANSITIONING_TO_BLUE" | "TRANSITIONING_TO_GREEN"
		deploymentReadyTimestamp?: string
		error?:                    string
		jobStatus?: {
			checkpointInfo?: {
				formatType?: "FULL" | "INCREMENTAL" | "UNKNOWN"
				lastCheckpoint?: {
					formatType?:   "FULL" | "INCREMENTAL" | "UNKNOWN"
					timeStamp?:    int
					triggerNonce?: int
					triggerType?:  "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
				}
				lastPeriodicCheckpointTimestamp?: int
				triggerId?:                       string
				triggerTimestamp?:                int
				triggerType?:                     "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
			}
			jobId?:   string
			jobName?: string
			savepointInfo?: {
				formatType?:                     "CANONICAL" | "NATIVE" | "UNKNOWN"
				lastPeriodicSavepointTimestamp?: int
				lastSavepoint?: {
					formatType?:   "CANONICAL" | "NATIVE" | "UNKNOWN"
					location?:     string
					timeStamp?:    int
					triggerNonce?: int
					triggerType?:  "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
				}
				savepointHistory?: [...{
					formatType?:   "CANONICAL" | "NATIVE" | "UNKNOWN"
					location?:     string
					timeStamp?:    int
					triggerNonce?: int
					triggerType?:  "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
				}]
				triggerId?:        string
				triggerTimestamp?: int
				triggerType?:      "MANUAL" | "PERIODIC" | "UNKNOWN" | "UPGRADE"
			}
			startTime?:            string
			state?:                "CANCELED" | "CANCELLING" | "CREATED" | "FAILED" | "FAILING" | "FINISHED" | "INITIALIZING" | "RECONCILING" | "RESTARTING" | "RUNNING" | "SUSPENDED"
			updateTime?:           string
			upgradeSavepointPath?: string
		}
		lastReconciledSpec?:      string
		lastReconciledTimestamp?: string
		savepointTriggerId?:      string
	}

	_embeddedResource: {
		apiVersion!: string
		kind!:       string
		metadata?: {
			...
		}
	}
	apiVersion: "flink.apache.org/v1beta1"
	kind:       "FlinkBlueGreenDeployment"
	metadata!: {
		name!:      string
		namespace!: string
		labels?: [string]:      string
		annotations?: [string]: string
		...
	}
}
