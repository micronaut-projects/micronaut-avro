package io.micronaut.avro.serde;

import java.io.IOException;

@FunctionalInterface
interface EncodingRunnable {

    void run() throws IOException;

}
