rep = '____'

fwd = {
    'max-sys-jobs': 'mapred.capacity-scheduler.maximum-system-jobs',
    'supports-priority': \
        'mapred.capacity-scheduler.default-supports-priority',
    'min-user-lim-perc': \
        'mapred.capacity-scheduler.default-minimum-user-limit-percent',
    'user-lim-factor': \
        'mapred.capacity-scheduler.default-user-limit-factor',
    'max-tasks-per-queue': \
        'mapred.capacity-scheduler.default-maximum-active-tasks-per-queue',
    'max-tasks-per-user': \
        'mapred.capacity-scheduler.default-maximum-active-tasks-per-user',
    'accept-jobs-factor': \
        'mapred.capacity-scheduler.default-init-accept-jobs-factor',
    'poll-interval': 'mapred.capacity-scheduler.init-poll-interval',
    'worker-threads': 'mapred.capacity-scheduler.init-worker-threads',
    'admins': 'mapred.queue.' + queue_rep + '.acl-submit-job',
    'users': 'mapred.queue.' + queue_rep + '.acl-administer-jobs',
    'capacity': 'mapred.capacity-scheduler.queue.' + queue_rep + '.capacity',
    'max-cap': 'mapred.capacity-scheduler.queue.' + queue_rep + \
        '.maximum-capacity',
    'max-tpu': 'mapred.capacity-scheduler.queue.' + queue_rep + \
            '.maximum-initialized-active-tasks-per-user',
}
