class HadoopMapper:

    rep = '____'
    field_sep = '.'
    mapping = {
            1: {
                'queues': {
                    'admins': 'mapred.queue.' + rep + '.acl-administer-jobs',
                    'maxcap': 'mapred.capacity-scheduler.queue.' + rep + \
                            '.maximum-capacity',
                    'maxtpu': 'mapred.capacity-scheduler.queue.' + rep + \
                            '.maximum-initialized-active-tasks-per-user',
                    'mincap': 'mapred.capacity-scheduler.queue.' + rep + \
                            '.capacity',
                    'ulim': 'mapred.capacity-scheduler.queue.' + rep + \
                            '.minimum-user-limit-percent',
                    'users': 'mapred.queue.' + rep + '.acl-submit-job'
                    },

                'scheduler': {
                    'mapred.capacity-scheduler.default-init-accept-jobs-factor': \
                        'mapred.capacity-scheduler.default-init-accept-jobs-factor',
                    'mapred.capacity-scheduler.default-supports-priority': \
                        'mapred.capacity-scheduler.default-supports-priority',
                    'mapred.capacity-scheduler.default-user-limit-factor': \
                        'mapred.capacity-scheduler.default-user-limit-factor',
                    'mapred.capacity-scheduler.init-poll-interval': \
                        'mapred.capacity-scheduler.init-poll-interval',
                    'mapred.capacity-scheduler.init-worker-threads': \
                        'mapred.capacity-scheduler.init-worker-threads',
                    'maxjobs': 'mapred.capacity-scheduler.maximum-system-jobs',
                    'maxtpq': 'mapred.capacity-scheduler.default-maximum-active-tasks-per-queue',
                    'maxtpu': 'mapred.capacity-scheduler.default-maximum-active-tasks-per-user',
                    'ulim': 'mapred.capacity-scheduler.default-minimum-user-limit-percent'
                    }
                },
            2: {
                'queues': {
                    'admins': 'yarn.scheduler.capacity.root.' + rep + '.acl_administer_queue',
                    'maxcap': 'yarn.scheduler.capacity.root.' + rep + '.maximum-capacity',
                    'mincap': 'yarn.scheduler.capacity.root.' + rep + '.capacity',
                    'state': 'yarn.scheduler.capacity.root.' + rep + '.state',
                    'ulim': 'yarn.scheduler.capacity.root.' + rep + '.user-limit-factor',
                    'users': 'yarn.scheduler.capacity.root.' + rep + '.acl_submit_applications'
                    },

                'scheduler': {
                    'maxjobs': 'yarn.scheduler.capacity.maximum-applications',
                    'yarn.scheduler.capacity.maximum-am-resource-percent': \
                        'yarn.scheduler.capacity.maximum-am-resource-percent',
                    'yarn.scheduler.capacity.node-locality-delay': \
                        'yarn.scheduler.capacity.node-locality-delay',
                    'yarn.scheduler.capacity.resource-calculator': \
                        'yarn.scheduler.capacity.resource-calculator'
                    }
                }
            }

    def __getitem__(self, args):
        """ args is a tuple of the format:

        (key, ver) or (key, ver, owner) to get the correct key from the
        map
        """

        if len(args) != 3:
            raise KeyError('You must specify a key, version, and owner')

        key = args[0]
        ver = args[1]
        owner = args[2]

        tmp_map = HadoopMapper.mapping[ver][owner]

        if key not in tmp_map.keys():
            raise KeyError('Hadoop version ' + str(ver) + ' does not have' + \
                    ' support for ' + key)

        return tmp_map[key]

    def versions(self):
        return sorted(HadoopMapper.mapping.keys())
