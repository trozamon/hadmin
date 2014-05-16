class HadoopMapper:

    rep = '____'
    field_sep = '.'
    mapping = {
            1: {
                'max-jobs': {
                    'scheduler': 'mapred.capacity-scheduler.maximum-system-jobs'
                    },

                'user-limit-factor': {
                    'scheduler': 'mapred.capacity-scheduler.default-user-limit-factor'
                    },

                'max-tpq': {
                    'scheduler': 'mapred.capacity-scheduler.default-maximum-active-tasks-per-queue'
                    },

                'max-tpu': {
                    'scheduler': 'mapred.capacity-scheduler.default-maximum-active-tasks-per-user',
                    'queues': 'mapred.capacity-scheduler.queue.' + rep + '.maximum-initialized-active-tasks-per-user'
                    },

                'admins': {
                    'queues': 'mapred.queue.' + rep + '.acl-administer-jobs'
                    },

                'users': {
                    'queues': 'mapred.queue.' + rep + '.acl-submit-job'
                    },

                'cap': {
                    'scheduler': 'mapred.capacity-scheduler.queue.' + rep + '.capacity'
                    },

                'max-cap': {
                    'scheduler': 'mapred.capacity-scheduler.queue.' + rep + '.maximum-capacity'
                    }
                },
            2: {
                'max-jobs': {
                    'scheduler': 'yarn.scheduler.capacity.maximum-applications'
                    },

                'user-limit-factor': {
                    'scheduler': 'yarn.scheduler.capacity.root.default.user-limit-factor'
                    },

                'admins': {
                    'queues': 'yarn.scheduler.capacity.root.' + rep + '.acl_administer_queue'
                    },

                'users': {
                    'queues': 'yarn.scheduler.capacity.root.' + rep + '.acl_submit_applications'
                    },

                'cap': {
                    'scheduler': 'yarn.scheduler.capacity.root.' + rep + '.capacity'
                    },

                'max-cap': {
                    'scheduler': 'yarn.scheduler.capacity.root.' + rep + '.maximum-capacity'
                    }
                }
            }

    def __getitem__(self, args):
        """ args is a tuple of the format:

        (key, ver) or (key, ver, owner) to get the correct key from the
        map
        """

        ver = args[1]
        key = args[0]
        owner = None
        if len(args) == 3:
            owner = args[2]

        vermap = HadoopMapper.mapping[ver]

        if key not in vermap.keys():
            raise KeyError('Hadoop version ' + str(ver) + ' does not have' + \
                    ' support for ' + key)

        keymap = vermap[key]
        if owner is None:
            if len(keymap.keys()) == 1:
                owner = list(keymap.keys())[0]
            else:
                raise KeyError('You must specify an owner for key ' + key)
        elif owner not in keymap.keys():
            raise KeyError('Mapping is screwed up somewhere')

        return keymap[owner]

    def versions(self):
        return sorted(HadoopMapper.mapping.keys())
