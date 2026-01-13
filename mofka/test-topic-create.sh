#/bin/zsh
set -eu

# TEST TOPIC CREATE
# Create a topic

if (( ${#*} != 2 )) {
  print "topic-create: Provide ROOT_PATH TOPIC"
  return 1
}

ROOT_PATH=$1
TOPIC=$2

diaspora-ctl topic create --name $TOPIC \
                          --driver files \
                          --driver.root_path $ROOT_PATH  \
                          --topic.num_partitions 1
