# these packages will be installed to master node

set -e

sudo apt-get update
if ! which git; then
  sudo apt-get install -y git
fi

if ! which java; then
  sudo apt-get install -y openjdk-7-jdk
fi

# Edit bash profile
echo "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64" >> ~/.bash_profile

source ~/.bash_profile

# Build Hadoop to install native libs
sudo mkdir -p ~/hadoop-native
cd /tmp

wget "http://archive.apache.org/dist/hadoop/common/hadoop-2.4.1/hadoop-2.4.1.tar.gz"
tar xvzf hadoop-2.4.1.tar.gz
cd hadoop-2.4.1
sudo cp lib/native/* ~/hadoop-native
rm -R /tmp/hadoop-2.4.1.tar.gz /tmp/hadoop-2.4.1

sudo apt-get install -y snappy
