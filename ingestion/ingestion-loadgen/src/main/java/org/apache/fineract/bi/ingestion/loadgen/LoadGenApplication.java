/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.bi.ingestion.loadgen;

import java.nio.file.Path;
import org.apache.fineract.bi.ingestion.contract.Contract;
import org.apache.fineract.bi.ingestion.contract.ContractLoader;

public final class LoadGenApplication {

    private LoadGenApplication() {
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: loadgen produce|verify|source-workload [--name=value ...]");
            System.exit(2);
        }
        Args args = new Args(argv);
        String contractPath = args.get("contract", System.getenv().getOrDefault("FBI_CONTRACT_PATH",
                ContractLoader.DEFAULT_PATH));
        switch (argv[0]) {
            case "produce" -> EventProducer.run(args, load(contractPath));
            case "verify" -> System.exit(Verifier.run(args, load(contractPath)));
            case "source-workload" -> SourceWorkload.run(args);
            default -> {
                System.err.println("Unknown command " + argv[0]);
                System.exit(2);
            }
        }
    }

    private static Contract load(String path) {
        return ContractLoader.load(Path.of(path));
    }
}
