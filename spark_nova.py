#!/usr/bin/env python
# -*- coding: utf-8 -*-

from optparse import OptionParser
import os
import os.path
import pipes
from stat import S_IRUSR
from sys import stderr
import sys
import subprocess
import tempfile
import textwrap
import time
import urllib2

import novaclient.client

SPARK_NOVA_VERSION = "1.0"
SPARK_NOVA_DIR = os.path.dirname(os.path.realpath(__file__))

VALID_SPARK_VERSIONS = set([
    "0.7.3",
    "0.8.0",
    "0.8.1",
    "0.9.0",
    "0.9.1",
    "0.9.2",
    "1.0.0",
    "1.0.1",
    "1.0.2",
    "1.1.0",
    "1.1.1",
    "1.2.0",
    "1.2.1",
    "1.3.0",
])

DEFAULT_SPARK_VERSION = "1.3.0"
DEFAULT_SPARK_GITHUB_REPO = "https://github.com/apache/spark"

# Default location to get the spark-nova scripts from
DEFAULT_SPARK_EC2_GITHUB_REPO = "https://github.com/liuj64/spark-ec2"
DEFAULT_SPARK_EC2_BRANCH = "branch-1.3"

class UsageError(Exception):
    pass

def parse_args():
    parser = OptionParser(
        prog="spark-nova",
        version="%prog {v}".format(v=SPARK_NOVA_VERSION),
        usage="%prog [options] <action> <cluster_name>\n\n"
        + "<action> can be: launch, destroy, login, stop, start, get-master, reboot-slaves")

    parser.add_option(
        "-s", "--slaves", type="int", default=1,
        help="Number of slaves to launch (default: %default)")
    parser.add_option(
        "-k", "--key-pair",
        help="Key pair to use on instances")
    parser.add_option(
        "-i", "--identity-file",
        help="SSH private key file to use for logging into instances")
    parser.add_option(
        "-u", "--user", default="root",
        help="The SSH user you want to connect as (default: %default)")
    parser.add_option(
        "-v", "--spark-version", default=DEFAULT_SPARK_VERSION,
        help="Version of Spark to use: 'X.Y.Z' or a specific git hash (default: %default)")
    parser.add_option(
        "--spark-git-repo",
        default=DEFAULT_SPARK_GITHUB_REPO,
        help="Github repo from which to checkout supplied commit hash (default: %default)")
    parser.add_option(
        "--spark-ec2-git-repo",
        default=DEFAULT_SPARK_EC2_GITHUB_REPO,
        help="Github repo from which to checkout spark-ec2 (default: %default)")
    parser.add_option(
        "--spark-ec2-git-branch",
        default=DEFAULT_SPARK_EC2_BRANCH,
        help="Github repo branch of spark-ec2 to use (default: %default)")
    parser.add_option(
        "--deploy-root-dir",
        default=None,
        help="A directory to copy into / on the first master. " +
             "Must be absolute. Note that a trailing slash is handled as per rsync: " +
             "If you omit it, the last directory of the --deploy-root-dir path will be created " +
             "in / before copying its contents. If you append the trailing slash, " +
             "the directory is not created and its contents are copied directly into /. " +
             "(default: %default).")
    parser.add_option(
        "--hadoop-major-version", default="1",
        help="Major version of Hadoop (default: %default)")
    parser.add_option(
        "--resume", action="store_true", default=False,
        help="Resume installation on a previously launched cluster " +
             "(for debugging)")
    parser.add_option(
        "--worker-instances", type="int", default=1,
        help="Number of instances per worker: variable SPARK_WORKER_INSTANCES (default: %default)")
    parser.add_option(
        "--master-opts", type="string", default="",
        help="Extra options to give to master through SPARK_MASTER_OPTS variable " +
             "(e.g -Dspark.worker.timeout=180)")
    parser.add_option(
        "--prebuilt-image", action="store_true", default=False,
        help="Prebuilt image with necessary software installed like Git,Java")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args

    return (opts, action, cluster_name)

def get_validate_spark_version(version, repo):
    if "." in version:
        version = version.replace("v", "")
        if version not in VALID_SPARK_VERSIONS:
            print >> stderr, "Don't know about Spark version: {v}".format(v=version)
            sys.exit(1)
        return version
    else:
        github_commit_url = "{repo}/commit/{commit_hash}".format(repo=repo, commit_hash=version)
        request = urllib2.Request(github_commit_url)
        request.get_method = lambda: 'HEAD'
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, e:
            print >> stderr, "Couldn't validate Spark commit: {url}".format(url=github_commit_url)
            print >> stderr, "Received HTTP response code of {code}.".format(code=e.code)
            sys.exit(1)
        return version

def is_ssh_available(host, opts, print_ssh_output=True):
    """
    Check if SSH is available on a host.
    """
    s = subprocess.Popen(
        ssh_command(opts) + ['-t', '-t', '-o', 'ConnectTimeout=3',
                             '%s@%s' % (opts.user, host), stringify_command('true')],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT  # we pipe stderr through stdout to preserve output order
    )
    cmd_output = s.communicate()[0]  # [1] is stderr, which we redirected to stdout

    if s.returncode != 0 and print_ssh_output:
        # extra leading newline is for spacing in wait_for_cluster_state()
        print textwrap.dedent("""\n
            Warning: SSH connection error. (This could be temporary.)
            Host: {h}
            SSH return code: {r}
            SSH output: {o}
        """).format(
            h=host,
            r=s.returncode,
            o=cmd_output.strip()
        )

    return s.returncode == 0

def check_instance_state(client, instance):
    attempt = 0
    while True:
        time.sleep(5*attempt)
        attempt += 1
        print 'attempt:', attempt
        server = client.servers.get(instance.id)
        print server.status
        if server.status == 'ACTIVE':
            break
        elif server.status == 'ERROR':
            raise Exception('start VM failed')

def wait_ssh_available(ip, opts):
    attempt = 0
    while True:
        time.sleep(5*attempt)
        attempt += 1
        if is_ssh_available(ip, opts):
            break

def get_existing_cluster(client, opts, cluster_name):
    """
    Get the Nova instances
    """
    # search_opts
    servers = client.servers.list()
    # get instances
    master_instances = [s for s in servers if 
                        (cluster_name + '-spark-master') in s.name] #and s.status == 'ACTIVE']
    slave_instances = [s for s in servers if 
                       (cluster_name + '-spark-slave') in s.name] #and s.status == 'ACTIVE']

    return (master_instances, slave_instances)

def get_security_group(client, name):
    sgs = client.security_groups.list()
    group = [g for g in sgs if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print "Creating security group " + name
        secgroup = client.security_groups.create(name, "Spark Nova group")

        rulesets = [
            {
                # ssh
                'ip_protocol': 'tcp', 'from_port': 22, 'to_port': 22, 'cidr': '0.0.0.0/0',
            },
            {
                # ping
                'ip_protocol': 'icmp', 'from_port': -1,'to_port': -1, 'cidr': '0.0.0.0/0',
            }
        ]
        if(name.endswith('master')):
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 8080, 'to_port': 8081, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 18080, 'to_port': 18080, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 19999, 'to_port': 19999, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 50030, 'to_port': 50030, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 50070, 'to_port': 50070, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 60070, 'to_port': 60070, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 4040, 'to_port': 4045, 'cidr': '0.0.0.0/0'})
            # ganglia:
            # rulesets.append({'ip_protocol': 'tcp', 'from_port': 5080,
            #                   'to_port': 5080, 'cidr': '0.0.0.0/0'})
        elif(name.endswith('slaves')):
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 8080, 'to_port': 8081, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 50060, 'to_port': 50060, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 50075, 'to_port': 50075, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 60060, 'to_port': 60060, 'cidr': '0.0.0.0/0'})
            rulesets.append({'ip_protocol': 'tcp', 'from_port': 60075, 'to_port': 60075, 'cidr': '0.0.0.0/0'})
        else:
            pass

        for ruleset in rulesets:
            client.security_group_rules.create(secgroup.id, **ruleset)

        return secgroup

def launch_cluster(compute_client, opts, cluster_name):
    masters, slaves = get_existing_cluster(compute_client, opts, cluster_name)
    if slaves or masters:
        print >> stderr, ("ERROR: There are already instances running in " +
                          "group %s" % cluster_name)
        sys.exit(1)

    # add security group
    master_sec_groups = []
    slave_sec_groups = []
    master_sec_groups.append(get_security_group(
                            compute_client, cluster_name + "-spark-master").name)
    slave_sec_groups.append(get_security_group(
                            compute_client, cluster_name + "-spark-slaves").name)

    name = cluster_name + '-spark-master'
    image = os.environ['VM_IMAGE_REF']
    flavor = os.environ['VM_FLAVOR_ID']
    # the syntax for block_device_mapping is
    # dev_name=id:type:size:delete_on_terminate
    # bd_map = {'vda': vol_id + ':::0'}
    create_kwargs = {
        # 'block_device_mapping': bd_map
    }
    master = compute_client.servers.create(
                name, image, flavor,
                security_groups=master_sec_groups,
                key_name=opts.key_pair, **create_kwargs)
    print master.status
    check_instance_state(compute_client, master)
    floating_ip = compute_client.floating_ips.create()
    print 'floating_ip:', floating_ip
    master.add_floating_ip(floating_ip)
    master_ips = [ floating_ip.ip ]

    slave_ips = []
    for i in range(0, opts.slaves):
        name = cluster_name + '-spark-slave-' + str(i)
        slave = compute_client.servers.create(
                name, image, flavor,
                security_groups=slave_sec_groups,
                key_name=opts.key_pair, **create_kwargs)
        check_instance_state(compute_client, slave)
        floating_ip = compute_client.floating_ips.create()
        print 'floating_ip:', floating_ip
        slave.add_floating_ip(floating_ip)
        slave_ips.append(floating_ip.ip)

    return master_ips, slave_ips

def setup_cluster(master_ips, slave_ips, opts, deploy_ssh_key):
    master = master_ips[0]
    if deploy_ssh_key:
        print "Generating cluster's SSH key on master..."
        key_setup = """
          [ -f ~/.ssh/id_rsa ] ||
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
        """
        ssh(master, opts, key_setup)
        dot_ssh_tar = ssh_read(master, opts, ['tar', 'c', '.ssh'])
        print "Transferring cluster's SSH key to slaves..."
        for slave_ip in slave_ips:
            print slave_ip
            ssh_write(slave_ip, opts, ['tar', 'x'], dot_ssh_tar)

    modules = ['spark']

    if not opts.prebuilt_image:
        # Install packages on master
        pkg_inst_script = None
        with open(os.path.join(SPARK_NOVA_DIR, "pre_setup.sh")) as pkg_inst_file:
            pkg_inst_script = pkg_inst_file.read()
        ssh(host=master, opts=opts, command=str(pkg_inst_script))

    # NOTE: We should clone the repository before running deploy_files to
    # prevent variables.sh from being overwritten
    print "Cloning spark-nova scripts from {r}/tree/{b} on master...".format(
        r=opts.spark_ec2_git_repo, b=opts.spark_ec2_git_branch)
    ssh(
        host=master,
        opts=opts,
        command="rm -rf spark-ec2"
        + " && "
        + "git clone {r} -b {b} spark-ec2".format(r=opts.spark_ec2_git_repo,
                                                  b=opts.spark_ec2_git_branch)
    )

    print "Deploying files to master..."
    deploy_files(
        root_dir=SPARK_NOVA_DIR + "/" + "deploy",
        opts=opts,
        master_ips=master_ips,
        slave_ips=slave_ips,
        modules=modules
    )

    if opts.deploy_root_dir is not None:
        print "Deploying {s} to master...".format(s=opts.deploy_root_dir)
        deploy_user_files(
            root_dir=opts.deploy_root_dir,
            opts=opts,
            master_nodes=master_nodes
        )

    print "Running setup on master..."
    setup_spark_cluster(master, opts)
    print "Done!"

def setup_spark_cluster(master, opts):
    ssh(master, opts, "chmod u+x spark-ec2/setup.sh")
    ssh(master, opts, "spark-ec2/setup.sh")
    print "Spark standalone cluster started at http://%s:8080" % master

# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of masters and slaves). Files are only deployed to
# the first master instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
#
# root_dir should be an absolute path to the directory with the files we want to deploy.
def deploy_files(root_dir, opts, master_ips, slave_ips, modules):
    active_master = master_ips[0]

    num_disks = 1 #get_num_disks(opts.instance_type)
    hdfs_data_dirs = "/mnt/ephemeral-hdfs/data"
    mapred_local_dirs = "/mnt/hadoop/mrlocal"
    spark_local_dirs = "/mnt/spark"
    if num_disks > 1:
        for i in range(2, num_disks + 1):
            hdfs_data_dirs += ",/mnt%d/ephemeral-hdfs/data" % i
            mapred_local_dirs += ",/mnt%d/hadoop/mrlocal" % i
            spark_local_dirs += ",/mnt%d/spark" % i

    cluster_url = "%s:7077" % active_master

    if "." in opts.spark_version:
        # Pre-built Spark deploy
        spark_v = get_validate_spark_version(opts.spark_version, opts.spark_git_repo)
        tachyon_v = "" #get_tachyon_version(spark_v)
    else:
        # Spark-only custom deploy
        spark_v = "%s|%s" % (opts.spark_git_repo, opts.spark_version)
        tachyon_v = ""
        print "Deploying Spark via git hash; Tachyon won't be set up"
        modules = filter(lambda x: x != "tachyon", modules)

    template_vars = {
        "master_list": '\n'.join([i for i in master_ips]),
        "active_master": active_master,
        "slave_list": '\n'.join([i for i in slave_ips]),
        "cluster_url": cluster_url,
        "hdfs_data_dirs": hdfs_data_dirs,
        "mapred_local_dirs": mapred_local_dirs,
        "spark_local_dirs": spark_local_dirs,
        "modules": '\n'.join(modules),
        "spark_version": spark_v,
        "tachyon_version": tachyon_v,
        "hadoop_major_version": opts.hadoop_major_version,
        "spark_worker_instances": "%d" % opts.worker_instances,
        "spark_master_opts": opts.master_opts
    }

    # Create a temp directory in which we will place all the files to be
    # deployed after we substitue template parameters in them
    tmp_dir = tempfile.mkdtemp()
    print 'tmp_dir:', tmp_dir
    for path, dirs, files in os.walk(root_dir):
        if path.find(".svn") == -1:
            dest_dir = os.path.join('/', path[len(root_dir):])
            local_dir = tmp_dir + dest_dir
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            for filename in files:
                if filename[0] not in '#.~' and filename[-1] != '~':
                    dest_file = os.path.join(dest_dir, filename)
                    local_file = tmp_dir + dest_file
                    with open(os.path.join(path, filename)) as src:
                        with open(local_file, "w") as dest:
                            text = src.read()
                            for key in template_vars:
                                text = text.replace("{{" + key + "}}", template_vars[key])
                            dest.write(text)
                            dest.close()
    # rsync the whole directory over to the master machine
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s/" % tmp_dir,
        "%s@%s:/" % (opts.user, active_master)
    ]
    subprocess.check_call(command)
    # Remove the temp directory we created above
    shutil.rmtree(tmp_dir)


# Deploy a given local directory to a cluster, WITHOUT parameter substitution.
# Note that unlike deploy_files, this works for binary files.
# Also, it is up to the user to add (or not) the trailing slash in root_dir.
# Files are only deployed to the first master instance in the cluster.
#
# root_dir should be an absolute path.
def deploy_user_files(root_dir, opts, master_ips):
    active_master = master_ips[0]
    command = [
        'rsync', '-rv',
        '-e', stringify_command(ssh_command(opts)),
        "%s" % root_dir,
        "%s@%s:/" % (opts.user, active_master)
    ]
    subprocess.check_call(command)

def stringify_command(parts):
    if isinstance(parts, str):
        return parts
    else:
        return ' '.join(map(pipes.quote, parts))


def ssh_args(opts):
    parts = ['-o', 'StrictHostKeyChecking=no']
    parts += ['-o', 'UserKnownHostsFile=/dev/null']
    if opts.identity_file is not None:
        parts += ['-i', opts.identity_file]
    return parts


def ssh_command(opts):
    return ['ssh'] + ssh_args(opts)


# Run a command on a host through ssh, retrying up to five times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                ssh_command(opts) + ['-t', '-t', '%s@%s' % (opts.user, host),
                                     stringify_command(command)])
        except subprocess.CalledProcessError as e:
            if tries > 5:
                # If this was an ssh failure, provide the user with hints.
                if e.returncode == 255:
                    raise UsageError(
                        "Failed to SSH to remote host {0}.\n" +
                        "Please check that you have provided the correct --identity-file and " +
                        "--key-pair parameters and try again.".format(host))
                else:
                    raise e
            print >> stderr, \
                "Error executing remote command, retrying after 30 seconds: {0}".format(e)
            time.sleep(30)
            tries = tries + 1


# Backported from Python 2.7 for compatiblity with 2.6 (See SPARK-1990)
def _check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        raise subprocess.CalledProcessError(retcode, cmd, output=output)
    return output


def ssh_read(host, opts, command):
    return _check_output(
        ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)])


def ssh_write(host, opts, command, arguments):
    tries = 0
    while True:
        proc = subprocess.Popen(
            ssh_command(opts) + ['%s@%s' % (opts.user, host), stringify_command(command)],
            stdin=subprocess.PIPE)
        proc.stdin.write(arguments)
        proc.stdin.close()
        status = proc.wait()
        if status == 0:
            break
        elif tries > 5:
            raise RuntimeError("ssh_write failed with error %s" % proc.returncode)
        else:
            print >> stderr, \
                "Error {0} while executing remote command, retrying after 30 seconds".format(status)
            time.sleep(30)
            tries = tries + 1

def get_floating_ip(compute_client, instance):
    ips = [ip for ip in compute_client.floating_ips.list() if instance.id == ip.instance_id]
    return ips[0].ip

def real_main():
    (opts, action, cluster_name) = parse_args()

    # Input parameter validation
    get_validate_spark_version(opts.spark_version, opts.spark_git_repo)

    if opts.identity_file is not None:
        if not os.path.exists(opts.identity_file):
            print >> stderr,\
                "ERROR: The identity file '{f}' doesn't exist.".format(f=opts.identity_file)
            sys.exit(1)

        file_mode = os.stat(opts.identity_file).st_mode
        if not (file_mode & S_IRUSR) or not oct(file_mode)[-2:] == '00':
            print >> stderr, "ERROR: The identity file must be accessible only by you."
            print >> stderr, 'You can fix this with: chmod 400 "{f}"'.format(f=opts.identity_file)
            sys.exit(1)

    if not (opts.deploy_root_dir is None or
            (os.path.isabs(opts.deploy_root_dir) and
             os.path.isdir(opts.deploy_root_dir) and
             os.path.exists(opts.deploy_root_dir))):
        print >> stderr, "--deploy-root-dir must be an absolute path to a directory that exists " \
                         "on the local file system"
        sys.exit(1)

    username = os.environ['OS_USERNAME']
    password = os.environ['OS_PASSWORD']
    tenant_name = os.environ['OS_TENANT_NAME']
    auth_url = os.environ['OS_AUTH_URL']
    client_args = (username, password, tenant_name, auth_url)

    service_type = 'compute'
    endpoint_type = 'publicURL'
    region = 'RegionOne'
    api_version = 2
    compute_client = novaclient.client.Client(api_version,
                                        *client_args,
                                        service_type=service_type,
                                        endpoint_type=endpoint_type,
                                        region_name=region,
                                        no_cache=True,
                                        insecure=True,
                                        http_log_debug=True)

    if action == "launch":
        if opts.slaves <= 0:
            print >> sys.stderr, "ERROR: You have to start at least 1 slave"
            sys.exit(1)
        if opts.resume:
            (master_nodes, slave_nodes) = get_existing_cluster(compute_client, opts, cluster_name)
            master_ips = [get_floating_ip(s) for s in master_nodes]
            slave_ips = [get_floating_ip(s) for s in slave_nodes]
        else:
            (master_ips, slave_ips) = launch_cluster(compute_client, opts, cluster_name)

        for ip in master_ips+slave_ips:
            wait_ssh_available(ip, opts)
        setup_cluster(master_ips, slave_ips, opts, True)

    elif action == "destroy":
        (master_nodes, slave_nodes) = get_existing_cluster(compute_client, opts, cluster_name)
        if any(master_nodes + slave_nodes):
            print "ALL DATA ON ALL NODES WILL BE LOST!!"

        msg = "Are you sure you want to destroy the cluster {c}? (y/N) ".format(c=cluster_name)
        response = raw_input(msg)
        print 'response is:', response
        if response == "y":
            print "Terminating master..."
            for inst in master_nodes:
                print inst.name
                inst.delete()
            print "Terminating slaves..."
            for inst in slave_nodes:
                print inst.name
                inst.delete()

        # delete security groups
        time.sleep(20)
        sec_groups = []
        sec_groups.append(get_security_group(
                            compute_client, cluster_name + "-spark-master"))
        sec_groups.append(get_security_group(
                            compute_client, cluster_name + "-spark-slaves"))
        for group in sec_groups:
            print "Deleting security group " + group.name
            compute_client.security_groups.delete(group)

    elif action == "login":
        (master_nodes, slave_nodes) = get_existing_cluster(compute_client, opts, cluster_name)
        master = get_floating_ip(compute_client, master_nodes[0])
        print "Logging into master " + master + "..."
        subprocess.check_call(
            ssh_command(opts) + ['-t', '-t', "%s@%s" % (opts.user, master)])

    elif action == "stop":
        response = raw_input(
            "Are you sure you want to stop the cluster " +
            cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
            "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (master_nodes, slave_nodes) = get_existing_cluster(
                compute_client, opts, cluster_name)
            print "Stopping master..."
            for inst in master_nodes:  
                inst.stop()
            print "Stopping slaves..."
            for inst in slave_nodes:
                inst.stop()

    elif action == "start":
        (master_nodes, slave_nodes) = get_existing_cluster(compute_client, opts, cluster_name)
        print "Starting slaves..."
        for inst in slave_nodes:
            inst.start()
        print "Starting master..."
        for inst in master_nodes:
            inst.start()

        master_ips = [get_floating_ip(s) for s in master_nodes]
        slave_ips = [get_floating_ip(s) for s in slave_nodes]
        for ip in master_ips+slave_ips:
            wait_ssh_available(ip, opts)
        setup_cluster(master_ips, slave_ips, opts, False)

    else:
        print >> stderr, "Invalid action: %s" % action
        sys.exit(1)


def main():
    try:
        real_main()
    except UsageError, e:
        print >> stderr, "\nError:\n", e
        sys.exit(1)

if __name__ == "__main__":
    main()
