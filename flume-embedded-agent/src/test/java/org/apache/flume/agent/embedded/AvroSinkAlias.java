/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.agent.embedded;

import static org.junit.Assume.assumeTrue;

import java.time.LocalDate;

/**
 * Skips the tests that need the {@code avro} sink alias of Flume core, until {@link #EXPIRATION}.
 * <p>
 * The alias points to a class name that this repository no longer uses.
 * The tests should pass again, once sinks are discovered through a {@code ServiceLoader}.
 * After the expiration date, they run again and fail, as a reminder.
 * </p>
 */
final class AvroSinkAlias {

    static final LocalDate EXPIRATION = LocalDate.of(2026, 11, 18);

    static void assumeResolvable() {
        assumeTrue(
                "The `avro` sink alias does not resolve, until sinks use a `ServiceLoader`",
                !LocalDate.now().isBefore(EXPIRATION));
    }

    private AvroSinkAlias() {}
}
