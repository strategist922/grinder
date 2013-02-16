This directory contains a fork of storm-kafka-0.8.0-wip3

It has minimal modifications to support a replay archive.

This includes replacing scheme with kafkaScheme which passes all the source metadata to the bolt
ie. topic, hostname, port, partition, offset