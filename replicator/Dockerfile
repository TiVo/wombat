FROM stealthly/docker-java

MAINTAINER jcheng@tivo.com

# Adding binaries
ADD build/libs/replicator.jar /replicator.jar

# This creates the config file, and then runs the code.
ADD docker_entrypoint.sh /docker_entrypoint.sh

ENTRYPOINT ["/docker_entrypoint.sh"]
