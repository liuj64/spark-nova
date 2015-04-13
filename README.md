# spark-nova
Script for launching Spark clusters on Openstack Nova
This script is modified from spark-ec2, usage instructions of spark-ec2 are available online at: 
http://spark.apache.org/docs/latest/ec2-scripts.html

How to use this script:
1. Edit novarc for your Openstack Env and ImageId, FlavorId
2. Run command 
  # source novarc
3. Run spark-nova commands like spark-ec2, for example
  # ./spark-nova -k autokey -i autokey.pem -s 2 -u ubuntu launch test
  Add a parameter --prebuilt-image when you have an image built with essential software like Git,Java.
  Or spark-nova script will run pre_setup.sh to install these software after VM boot.

Current test is on Ubuntu 14.04. To use on other Linux distributions, please modify pre_setup.sh.
