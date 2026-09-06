[![progress-banner](https://backend.codecrafters.io/progress/kafka/77d2f227-88c8-458a-8ae8-da87ebb0d99c)](https://app.codecrafters.io/users/AnandMaurya24?r=2qF)

This is a starting point for Java solutions to the
["Build Your Own Kafka" Challenge](https://codecrafters.io/challenges/kafka).

In this challenge, you'll build a toy Kafka clone that's capable of accepting
and responding to ApiVersions & Fetch API requests. You'll also learn about
encoding and decoding messages using the Kafka wire protocol. You'll also learn
about handling the network protocol, event loops, TCP sockets and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Kafka implementation is in `src/main/java/Main.java`.
Study and uncomment the relevant code, and then run the command below to execute
the tests on our servers:

```sh
codecrafters submit
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `mvn` installed locally
1. Run `./your_program.sh` to run your Kafka broker, which is implemented in
   `src/main/java/Main.java`.
1. Run `codecrafters submit` to submit your solution to CodeCrafters. Test
   output will be streamed to your terminal.
