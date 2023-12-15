# SPDX-FileCopyrightText: 2023-present Timur Isanov <tisanov@evolution.com>
#
# SPDX-License-Identifier: MIT

from datetime import datetime, timezone, timedelta

import requests
from requests.compat import urljoin
from requests.exceptions import HTTPError


class AuthError(Exception):
    pass


class CassandraReaper:
    def __init__(self, url: str, user: str, password: str, verify_ssl=True, login=True) -> None:
        self.url = url
        self.__s = requests.session()
        self.__s.verify = verify_ssl
        self.user = user
        self.__password = password
        self.token = ''
        if login:
            self.login()

    def login(self) -> None:
        """Get jwt token"""
        data = {'username': self.user,
                'password': self.__password, 'rememberMe': False}
        login_url = urljoin(self.url, 'login')
        login_req = self.__s.post(login_url, data=data)
        self.__check_req(login_req)
        jwt_url = urljoin(self.url, 'jwt')
        jwt_req = self.__s.get(jwt_url)
        self.__check_req(jwt_req)
        self.token = jwt_req.text
        self.__s.headers.update({'Authorization': f"Bearer {self.token}"})

    def __check_req(self, req):
        if not req.ok:
            msg = f"URL: {req.url}, Status: {req.status_code}, Text: {req.text}"
            if req.status_code in (403, 498, 499):
                raise AuthError(msg)
            raise HTTPError(msg)

    def __auth_req(func):
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except AuthError:
                self.login()
                return func(self, *args, **kwargs)
        return wrapper

    @__auth_req
    def __get(self, query: str, params={}, timeout=10) -> dict:
        """Get request"""
        url = urljoin(self.url, query)
        req = self.__s.get(url, timeout=timeout, params=params)
        self.__check_req(req)
        return req

    @__auth_req
    def __delete(self, query: str, params={}, timeout=10) -> dict:
        """Delete request"""
        url = urljoin(self.url, query)
        req = self.__s.delete(url, params=params, timeout=timeout)
        self.__check_req(req)
        return req

    @__auth_req
    def __post(self, query: str, params={}, data={}, timeout=10) -> dict:
        """Post request"""
        url = urljoin(self.url, query)
        req = self.__s.post(url, params=params, data=data,
                            timeout=timeout)
        self.__check_req(req)
        return req

    @__auth_req
    def __put(self, query: str, params={}, data={}, timeout=10) -> dict:
        """Put request"""
        url = urljoin(self.url, query)
        req = self.__s.put(url, data=data, params=params, timeout=timeout)
        self.__check_req(req)
        return req

    @__auth_req
    def __patch(self, query: str, params={}, json={}, timeout=10) -> dict:
        """Patch request"""
        url = urljoin(self.url, query)
        req = self.__s.patch(url, json=json, params=params, timeout=timeout)
        self.__check_req(req)
        return req

    def update_password(self, new_password: str) -> None:
        self.__password = new_password

    def get_clusters(self, timeout=10) -> list:
        """Get list of Cassandra clusters"""
        req = self.__get('cluster', timeout=timeout)
        return req.json()

    def get_cluster_info(self, cluster: str, limit=200, timeout=30) -> dict:
        """Get information about Cassandra cluster"""
        req = self.__get(f"cluster/{cluster}",
                         {'limit': limit}, timeout=timeout)
        return req.json()

    def get_cluster_tables(self, cluster: str, timeout=10) -> dict:
        """Get dict of keyspaces and tables list"""
        req = self.__get(f"cluster/{cluster}/tables", timeout=timeout)
        return req.json()

    def delete_cluster(self, cluster: str, force=False, timeout=10) -> None:
        params = {'force': force}
        self.__delete(f"cluster/{cluster}", params=params, timeout=timeout)

    def get_repairs(self, cluster='', states=[], timeout=10) -> list:
        """Get last repairs"""
        params = {}
        if cluster:
            params['cluster_name'] = cluster
        if states:
            states_str = ",".join(states)
            params['state'] = states_str
        req = self.__get('repair_run', params=params, timeout=timeout)
        return req.json()

    def get_repair(self, id: str, timeout=10) -> dict:
        """Get running repair info"""
        req = self.__get(f"repair_run/{id}", timeout=timeout)
        return req.json()

    def pause_repair(self, id: str, timeout=10) -> None:
        """Pause running repair by id"""
        state = 'PAUSED'
        self.__put(f"/repair_run/{id}/state/{state}", timeout=timeout)

    def change_repair_intensity(self, id: str, intensity: float, timeout=10) -> None:
        self.__put(f"repair_run/{id}/intensity/{intensity}", timeout=timeout)

    def resume_repair(self, id: str, timeout=10) -> None:
        """Resume paused repair by id"""
        state = 'RUNNING'
        self.__put(f"repair_run/{id}/state/{state}", timeout=timeout)

    def abort_repair(self, id: str, timeout=10) -> None:
        """Abort repair by id"""
        state = 'ABORTED'
        self.__put(f"repair_run/{id}/state/{state}", timeout=timeout)

    def delete_repair(self, id: str, timeout=10) -> None:
        """Delete repair by id"""
        repair = self.get_repair(id)
        params = {'owner': repair['owner']}
        self.__delete(f"repair_run/{id}", params=params, timeout=timeout)

    def get_repair_segments(self, id: str, timeout=10) -> list:
        """Get running repair segments"""
        req = self.__get(f"repair_run/{id}/segments", timeout=timeout)
        return req.json()

    def abort_repair_segment(self, id: str, segment_id: str, timeout=10) -> None:
        """Aborts a running segment and puts it back in NOT_STARTED state. The segment will be processed again later during the lifetime of the repair run."""
        self.__post(
            f"repair_run/{id}/segments/abort/{segment_id}", timeout=timeout)

    def get_schedules(self, cluster='', keyspace='', timeout=10) -> list:
        """Get repair schedules"""
        params = {}
        if cluster:
            params['clusterName'] = cluster
        if keyspace:
            params['keyspace'] = keyspace
        req = self.__get('repair_schedule', params=params, timeout=timeout)
        return req.json()

    def get_cluster_schedules(self, cluster: str, timeout=10) -> list:
        """Get repair schedules of a cluster"""
        req = self.__get(f"repair_schedule/cluster/{cluster}", timeout=timeout)
        return req.json()

    def disable_schedule(self, id: str, timeout=10) -> None:
        """Disable repair schedule by id"""
        params = {'state': 'PAUSED'}
        self.__put(f"repair_schedule/{id}", params=params, timeout=timeout)

    def add_schedule(
        self,
        cluster: str,
        keyspace: str,
        owner: str,
        schedule_days_between: int,
        segment_count_per_node=0,
        intensity=0.0,
        repair_parallelism='DATACENTER_AWARE',
        repair_thread_count=1,
        nodes=[],
        schedule_trigger_time=datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1),
        datacenters=[],
        tables=[],
        blacklisted_tables=[],
        incremental_repair=False,
        adaptive=True,
        percent_unrepaired_threshold=-1,
        timeout=10
    ) -> None:
        params = {
            'clusterName': cluster,
            'keyspace': keyspace,
            'owner': owner,
            'scheduleDaysBetween': schedule_days_between,
            'repairParallelism': repair_parallelism,
            'incrementalRepair': incremental_repair,
            'scheduleTriggerTime': schedule_trigger_time.isoformat(),
            'repairThreadCount': repair_thread_count,
            'adaptive': adaptive,
            'percentUnrepairedThreshold': percent_unrepaired_threshold,
        }
        if tables:
            params['tables'] = tables
        if segment_count_per_node:
            params['segmentCountPerNode'] = segment_count_per_node
        if intensity:
            params['intensity'] = intensity
        if nodes:
            params['nodes'] = nodes
        if datacenters:
            params['datacenters'] = datacenters
        if blacklisted_tables:
            params['blacklistedTables'] = blacklisted_tables
        self.__post('repair_schedule', params=params, timeout=timeout)

    def update_schedule(
        self,
        id: str,
        owner: str,
        repair_parallelism: str,
        intensity: float,
        scheduled_days_between: int,
        segment_count_per_node: int,
        percent_unrepaired_threshold: int,
        adaptive: bool,
        timeout=10
    ) -> dict:
        json = {
            'owner': owner,
            'repair_parallelism': repair_parallelism,
            'intensity': intensity,
            'scheduled_days_between': scheduled_days_between,
            'segment_count_per_node': segment_count_per_node,
            'percent_unrepaired_threshold': percent_unrepaired_threshold,
            'adaptive': adaptive,
        }
        req = self.__patch(f"repair_schedule/{id}", json=json, timeout=timeout)
        return req.json()

    def delete_schedule(self, id: str, timeout=10) -> None:
        """Delete repair schedule"""
        schedule = self.get_schedule(id)
        params = {'owner': schedule['owner']}
        self.__delete(f"repair_schedule/{id}", params=params, timeout=timeout)

    def enable_schedule(self, id: str, timeout=10) -> None:
        """Enable repair schedule by id"""
        params = {'state': 'ACTIVE'}
        self.__put(f"repair_schedule/{id}", params=params, timeout=timeout)

    def get_schedule(self, id: str, timeout=10) -> dict:
        """Get repair schedule info"""
        req = self.__get(f"repair_schedule/{id}", timeout=timeout)
        return req.json()

    def start_schedule(self, id: str, timeout=10) -> None:
        """Start repair schedule by id"""
        self.__post(f"repair_schedule/start/{id}", timeout=timeout)

    def get_cluster_snapshots(self, cluster: str, timeout=10) -> list:
        """Get cluster snapshots"""
        req = self.__get(f"/snapshot/cluster/{cluster}", timeout=timeout)
        return req.json()

    def get_host_snapshots(self, cluster: str, host: str, timeout=10) -> list:
        """Get snapshots of a host of a cluster"""
        req = self.__get(f"/snapshot/{cluster}/{host}", timeout=timeout)
        return req.json()

    def create_cluster_snapshot(self, cluster: str, snapshot_name: str, owner: str, cause='', keyspace='', tables=[], timeout=10) -> None:
        """Create a snapshot on all hosts in a cluster, using the same name"""
        params = {
            'snapshot_name': snapshot_name,
            'owner': owner,
        }
        if keyspace:
            params['keyspace'] = keyspace
        if tables:
            params['tables'] = ",".join(tables)
        if cause:
            params['cause'] = cause
        self.__post(f"/snapshot/cluster/{cluster}",
                    params=params, timeout=timeout)

    def create_host_snapshot(self, cluster: str, host: str, snapshot_name: str, owner: str, cause='', keyspace='', tables=[], timeout=10) -> None:
        """Create a snapshot on a specific host"""
        params = {
            'snapshot_name': snapshot_name,
            'owner': owner,
        }
        if keyspace:
            params['keyspace'] = keyspace
        if tables:
            params['tables'] = ",".join(tables)
        if cause:
            params['cause'] = cause
        self.__post(f"/snapshot/{cluster}/{host}",
                    params=params, timeout=timeout)

    def delete_cluster_snapshot(self, cluster: str, snapshot_name: str, timeout=10) -> None:
        """Deletes a specific snapshot on all nodes in a given cluster"""
        self.__delete(
            f"/snapshot/cluster/{cluster}/{snapshot_name}", timeout=timeout)

    def delete_host_snapshot(self, cluster: str, host: str, snapshot_name: str, timeout=10) -> None:
        """Deletes a specific snapshot on all nodes in a given cluster"""
        self.__delete(
            f"/snapshot/{cluster}/{host}/{snapshot_name}", timeout=timeout)
