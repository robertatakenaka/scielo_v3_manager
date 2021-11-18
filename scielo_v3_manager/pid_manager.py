from scielo_v3_manager.v1 import local_models


class Manager:

    def __init__(self, **kwargs):
        pid_manager = kwargs.get("pid_manager")
        if pid_manager:
            self._pid_manager = pid_manager
            return

        name = kwargs.get("name")
        if name:
            timeout = kwargs.get("timeout")
            _engine_args = kwargs.get("_engine_args")
            self._pid_manager = local_models.Manager(
                name, timeout, _engine_args)
            return

        raise ValueError(
            "scielo_v3_manager.pid_manager.Manager requires `pid_manager`")

    def manage(self, v2, v3, aop, filename, doi, status, generate_v3):
        """
        Usa pid_manager_local
        """
        return self._pid_manager.manage(
            v2, v3, aop, filename, doi, status, generate_v3)

