import json
import os
import time
from enum import Enum
from typing import Dict, List, Optional

from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.rest import ApiException
from kubernetes.client.models.v1_delete_options import V1DeleteOptions

from loguru import logger

from pycalrissian.context import CalrissianContext
from pycalrissian.job import CalrissianJob, ContainerNames
from pycalrissian.utils import copy_from_volume

SIDECAR_PREFIXES = ("vault-agent", "istio-proxy", "otel-collector")
PENDING_LIMIT_SEC = 300  # 5 minutes

class JobStatus(Enum):
    ACTIVE = "active"
    FAILED = "failed"
    SUCCEEDED = "succeeded"
    KILLED = "killed"

class CalrissianExecution:
    def __init__(self, job: CalrissianJob, runtime_context: CalrissianContext) -> None:
        self.job = job
        self.runtime_context = runtime_context
        self.namespaced_job = None
        self.killed = False
        self.kill_cause = {"step": "job-setup", "exit_code": 0, "error_msg": "Execution Success"}

    def submit(self):
        """Submits the job to the cluster"""
        logger.info(f"submit job {self.job.job_name}")
        response = self.runtime_context.batch_v1_api.create_namespaced_job(
            self.runtime_context.namespace, self.job.to_k8s_job()
        )
        self.namespaced_job_name = self.job.job_name
        self.namespaced_job = response
        logger.info(f"job {self.job.job_name} submitted")

    def get_status2(self):
        """Returns the job status"""
        if self.killed:
            return JobStatus.KILLED
        try:
            # Prefer container status of the Job's pod
            pods = self.runtime_context.core_v1_api.list_namespaced_pod(
                namespace=self.runtime_context.namespace,
                label_selector=f"job-name={self.namespaced_job_name}",
                timeout_seconds=10,
            ).items
            if pods:
                p = pods[0]
                cs = next((cs for cs in (p.status.container_statuses or [])
                        if cs.name == ContainerNames.CALRISSIAN.value), None)
                if cs and cs.state and cs.state.terminated:
                    return (JobStatus.SUCCEEDED if cs.state.terminated.exit_code == 0
                            else JobStatus.FAILED)
                response = self.runtime_context.batch_v1_api.read_namespaced_job_status(
                    name=self.namespaced_job_name,
                    namespace=self.runtime_context.namespace,
                    pretty=True,
                )
                # Fall back to Job status if container hasn't terminated yet
                if response.status.active is None and response.status.start_time is None:
                    return JobStatus.ACTIVE
                if response.status.active:
                    return JobStatus.ACTIVE
                if response.status.succeeded:
                    return JobStatus.SUCCEEDED
                if response.status.failed:
                    return JobStatus.FAILED
            return None
        except ApiException as e:
            logger.error(f"Exception when calling get status: {e}\n")
            raise e

    def get_status(self):
        """Returns the job status"""
        if self.killed:
            return JobStatus.KILLED
        try:
            response = self.runtime_context.batch_v1_api.read_namespaced_job_status(
                name=self.namespaced_job_name,
                namespace=self.runtime_context.namespace,
                pretty=True,
            )
            if response.status.active is None and response.status.start_time is None:
                return JobStatus.ACTIVE
            if response.status.active:
                return JobStatus.ACTIVE
            if response.status.succeeded:
                return JobStatus.SUCCEEDED
            if response.status.failed:
                return JobStatus.FAILED
            return None
        except ApiException as e:
            logger.error(f"Exception when calling get status: {e}\n")
            raise e

    def is_complete(self) -> bool:
        """Returns True if the job execution is completed (success or failed)"""
        return self.get_status() in [
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.KILLED,
        ]

    def is_succeeded(self) -> bool:
        """Returns True if the job execution is completed and succeeded"""
        return self.get_status() in [JobStatus.SUCCEEDED]

    def is_active(self) -> bool:
        """Returns True if the job execution is on-going"""
        return self.get_status() in [JobStatus.ACTIVE]

    def if_failed(self) -> bool:
        """Returns True if the job execution failed"""
        return self.get_status() in [JobStatus.FAILED]

    def if_killed(self) -> bool:
        """Returns True if the job execution failed"""
        return self.get_status() in [JobStatus.KILLED]
    
    def get_output(self) -> Dict:
        """Returns the job output"""
        if self.is_succeeded():
            try:
                filename = self.get_file_from_volume(["output.json"])[0]
                with open(filename, "r") as staged_file:
                    return json.load(staged_file)
            except json.decoder.JSONDecodeError:
                logger.info("output.json cannot be decoded. Bad file format.")
                return {}
            except Exception as e:
                logger.info(f"output.json Bad file. Uncaught {e}")
                return {}
        return {}

    def get_usage_report(self) -> Dict:
        """Returns the job usage report"""
        if self.is_complete():
            try:
                filename = self.get_file_from_volume(["report.json"])[0]
                with open(filename, "r") as staged_file:
                    return json.load(staged_file)
            except json.decoder.JSONDecodeError:
                logger.info("report.json cannot be decoded. Bad file format.")
                return {}
            except Exception as e:
                logger.info(f"report.json Bad file. Uncaught {e}")
                return {}

        return {}

    def get_file_from_volume(self, filenames):

        volume = {
            "name": self.job.volume_calrissian_wdir,
            "persistentVolumeClaim": {
                "claimName": self.runtime_context.calrissian_wdir
            },
        }
        volume_mount = {
            "name": self.job.volume_calrissian_wdir,
            "mountPath": self.job.calrissian_base_path,
        }

        destination_path = "."
        copy_from_volume(
            context=self.runtime_context,
            volume=volume,
            volume_mount=volume_mount,
            source_paths=[
                os.path.join(self.job.calrissian_base_path, filename)
                for filename in filenames
            ],
            destination_path=destination_path,
            labels=self.runtime_context.labels
        )

        return [os.path.join(destination_path, filename) for filename in filenames]

    def get_log(self):
        """Returns the job execution log"""
        if self.is_complete():
            return self._get_container_log(ContainerNames.CALRISSIAN)
        return None

    def get_tool_logs(self):
        """stages the tool logs from k8s volume"""
        try:
            usage_report = self.get_usage_report()
            if "children" in usage_report.keys():
                self.get_file_from_volume(
                    [
                        os.path.join(self.job.calrissian_base_path, tool["name"] + ".log")
                        for tool in usage_report["children"]
                    ]
                )

                return [
                    os.path.join(".", tool["name"] + ".log")
                    for tool in usage_report["children"]
                ]
        except json.decoder.JSONDecodeError:
            logger.info("getting tool log decode. Bad file format.")
            return []
        except Exception as e:
            logger.info(f"output.json Bad file. Uncaught {e}")
            return []


    def _pick_workload_container_name(self, pod: V1Pod, preferred: Optional[str]) -> str:
        if preferred:
            return preferred
        for c in (pod.spec.containers or []):
            if not any(c.name.startswith(p) for p in SIDECAR_PREFIXES):
                return c.name
        return pod.spec.containers[0].name

    def _get_container_status(self, pod: V1Pod, name: str):
        for cs in (pod.status.container_statuses or []):
            if cs.name == name:
                return cs
        return None

    def _wait_until_container_starts(self, pod: V1Pod, container_name: str, timeout_s: int = 120):
        """Block until container is Running or Terminated, otherwise raise after timeout."""
        import time
        start = time.time()
        while True:
            cs = self._get_container_status(pod, container_name)
            if cs and cs.state and (cs.state.running or cs.state.terminated):
                return cs
            if time.time() - start > timeout_s:
                raise RuntimeError(f"Container {container_name} did not start within {timeout_s}s (state={cs.state if cs else None})")
            time.sleep(1)
            # refresh pod
            pod = self.runtime_context.core_v1_api.read_namespaced_pod(
                name=pod.metadata.name, namespace=self.runtime_context.namespace
            )

    def _get_container_log(self, container):
        try:
            logger.info(f"Getting logs for container: {str(container)}")
            # 1) pick the Job pod (newest if there are retries)
            pods_list = self.runtime_context.core_v1_api.list_namespaced_pod(
                namespace=self.runtime_context.namespace,
                label_selector=f"job-name={self.job.job_name}",
                timeout_seconds=10,
            )
            if not pods_list.items:
                raise RuntimeError(f"No pod found for job {self.job.job_name}")

            pod = sorted(
                pods_list.items,
                key=lambda p: (p.status.start_time or p.metadata.creation_timestamp),
                reverse=True,
            )[0]
            pod_name = pod.metadata.name

            # 2) choose the workload container (ignore sidecars)
            desired = container.value  # typically "calrissian"
            container_name = self._pick_workload_container_name(pod, desired)

            # 3) wait until Running or Terminated (avoids 400 PodInitializing)
            cs = self._wait_until_container_starts(pod, container_name)
            read_previous = bool(cs.state.running and cs.restart_count)

            # If the container is running now but has restarted in the past, you might want the prior logs:
            resp = self.runtime_context.core_v1_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.runtime_context.namespace,
                container=container_name,
                previous=read_previous,
                _return_http_data_only=True,
                _preload_content=False,
            )
            return resp.data.decode("utf-8")

        except ApiException as e:
            # Decode body safely (can be bytes)
            body = e.body
            if isinstance(body, (bytes, bytearray)):
                body_text = body.decode("utf-8", "ignore")
            else:
                body_text = body or ""

            # Handle common benign races gracefully
            if e.status == 400 and (
                "waiting to start" in body_text
                or "previous terminated container" in body_text
                or "not found" in body_text
            ):
                return ""


    def get_start_time(self):
        """Returns the start time"""
        try:
            response = self.runtime_context.batch_v1_api.read_namespaced_job_status(
                name=self.namespaced_job_name,
                namespace=self.runtime_context.namespace,
                pretty=True,
            )
            if response.status.start_time is not None:
                return response.status.start_time
            return None
        except ApiException as e:
            logger.error(f"Exception when calling get status: {e}\n")
            raise e

    def get_completion_time(self):
        """Returns either the completion time or the last transition time"""
        try:
            response = self.runtime_context.batch_v1_api.read_namespaced_job_status(
                name=self.namespaced_job_name,
                namespace=self.runtime_context.namespace,
                pretty=True,
            )
            if response.status.completion_time is not None:
                return response.status.completion_time
            if response.status.conditions is not None:
                return response.status.conditions[0].last_transition_time
            return None
        except ApiException as e:
            logger.error(f"Exception when calling get status: {e}\n")
            raise e

    # Helper to delete the job correctly
    def _delete_job(self, request_timeout: Optional[int] = None):
        kwargs = {
            "name": self.namespaced_job_name,
            "namespace": self.runtime_context.namespace,
            "body": V1DeleteOptions(
                propagation_policy="Foreground",
                grace_period_seconds=0,
            ),
        }
        if request_timeout is not None:
            kwargs["_request_timeout"] = request_timeout
        try:
            self.runtime_context.batch_v1_api.delete_namespaced_job(**kwargs)
        except ApiException as e:
            logger.error(f"failed to delete job {self.namespaced_job_name}: {e}")

    def monitor(self, interval: int = 5, grace_period=120, wall_time: Optional[int] = None) -> Optional[str]:
        """
        Monitors the job. Returns:
          - "OUT_OF_RESOURCES" if the pod stayed Pending > 5 minutes and scheduler shows insufficient cpu/memory
          - "UNKNOWN_ERROR" if Pending > 5 minutes but no clear scheduler reason
          - None otherwise (normal completion or other kill paths)
        """
        iterations = 0
        pending_since_ts: Optional[float] = None

        while True:
            status = self.get_status()  # may be None just after submit
            logger.info("Job status is {status}")
            if status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.KILLED):
                logger.info("execution is complete")
                if status is JobStatus.SUCCEEDED:
                    logger.info("the outcome is: success!")
                elif status is JobStatus.FAILED:
                    logger.info("the outcome is: failed!")
                elif status is JobStatus.KILLED:
                    logger.info("the outcome is: killed!")
                return None

            # -- NEW: inspect the Calrissian job pod to track Pending time
            pod = self._pick_job_pod()
            if pod and pod.status and pod.status.phase == "Pending":
                if pending_since_ts is None:
                    pending_since_ts = time.time()
                    logger.info(f"Pod {pod.metadata.name} entered Pending; starting 5m timer")
                elif time.time() - pending_since_ts >= PENDING_LIMIT_SEC:
                    # Pending time threshold: decide cause and kill
                    events = self._pod_events_text(pod.metadata.name).lower()
                    if ("insufficient memory" in events) or ("insufficient cpu" in events or ("Pod unscheduled" in events)):
                        error_msg = "OUT_OF_RESOURCES: Scheduler reports insufficient CPU or memory to place the pod."
                    else:
                        error_msg = "UNKNOWN_ERROR: Scheduler could not schedule the job. Please contact the admin."
                    logger.warning(f"Killing job {self.job.job_name} error message: {self.kill_cause}")
                    self.kill_cause["error_msg"] = error_msg
                    self.kill_cause["exit_code"] = -1
                    self.killed = True
                    self._delete_job(request_timeout=wall_time)
                    return self.kill_cause
            else:
                # reset the timer if we leave Pending (e.g., moved to Running, Succeeded, etc.)
                pending_since_ts = None

            if status is None:
                logger.info(f"job {self.job.job_name} not visible/active yet; waiting...")
            elif status is JobStatus.ACTIVE:
                logger.info(f"job {self.job.job_name} is active")

            time.sleep(interval)
            iterations += 1

            # Existing wall-time guard
            if wall_time is not None and iterations > int(wall_time / interval):
                logger.warning("reached wall time for execution, killing job")
                self.killed = True
                self.kill_cause["error_msg"] = "DEADLINE_EXCEEDED: Monitor wall time exceeded."
                self.kill_cause["exit_code"] = -1
                self._delete_job(request_timeout=wall_time)
                return self.kill_cause


    def get_waiting_pods(self) -> List[V1Pod]:
        pods_waiting = []

        response = self.runtime_context.core_v1_api.list_namespaced_pod(
            self.runtime_context.namespace
        )

        if response is not None:
            for pod in response.items:
                if pod.status.container_statuses and "job" in pod.metadata.name:
                    for con_status in pod.status.container_statuses:

                        if (
                            con_status.state.waiting
                            and con_status.state.waiting.reason in ["ImagePullBackOff", "ErrImagePull", "InvalidImageName"]
                        ):
                            pods_waiting.append(pod.metadata.name)

        return pods_waiting

    def _pick_job_pod(self) -> Optional[V1Pod]:
        pods = self.runtime_context.core_v1_api.list_namespaced_pod(
            namespace=self.runtime_context.namespace,
            label_selector=f"job-name={self.job.job_name}",
            timeout_seconds=10,
        ).items
        if not pods:
            logger.debug(f"No pods found yet for job {self.job.job_name}")
            return None
        pod = sorted(
            pods,
            key=lambda p: (p.status.start_time or p.metadata.creation_timestamp),
            reverse=True,
        )[0]
        return pod

    def _pod_events_text(self, pod_name: str) -> str:
        try:
            ev = self.runtime_context.core_v1_api.list_namespaced_event(
                namespace=self.runtime_context.namespace,
                field_selector=f"involvedObject.name={pod_name}",
                _request_timeout=10,
            )
            return "\n".join(f"{e.reason}: {e.message}" for e in (ev.items or []))
        except Exception as e:
            logger.debug(f"Could not fetch events for pod {pod_name}: {e}")
            return ""
