-- CreateTable
CREATE TABLE "blocks" (
    "timestamp" TIMESTAMP NOT NULL,
    "number" INTEGER NOT NULL,
    "hash" TEXT NOT NULL,
    "parent_hash" TEXT,
    "nonce" TEXT NOT NULL,
    "sha3_uncles" TEXT,
    "logs_bloom" TEXT,
    "transactions_root" TEXT,
    "state_root" TEXT,
    "receipts_root" TEXT,
    "miner" TEXT,
    "difficulty" DECIMAL(65,30),
    "total_difficulty" DECIMAL(65,30),
    "size" INTEGER,
    "extra_data" TEXT,
    "gas_limit" INTEGER,
    "gas_used" INTEGER,
    "transaction_count" INTEGER,
    "base_fee_per_gas" INTEGER,

    CONSTRAINT "blocks_pkey" PRIMARY KEY ("number")
);

-- CreateTable
CREATE TABLE "contracts" (
    "address" TEXT NOT NULL,
    "bytecode" TEXT,
    "function_sighashes" TEXT[],
    "is_erc20" BOOLEAN,
    "is_erc721" BOOLEAN,
    "block_timestamp" TIMESTAMP NOT NULL,
    "block_number" INTEGER NOT NULL,
    "block_hash" TEXT NOT NULL,

    CONSTRAINT "contracts_pkey" PRIMARY KEY ("address")
);

-- CreateTable
CREATE TABLE "transactions" (
    "hash" TEXT NOT NULL,
    "nonce" INTEGER NOT NULL,
    "transaction_index" INTEGER NOT NULL,
    "from_address" TEXT NOT NULL,
    "to_address" TEXT,
    "value" DECIMAL(65,30),
    "gas" INTEGER,
    "gas_price" INTEGER,
    "input" TEXT,
    "receipt_cumulative_gas_used" INTEGER,
    "receipt_gas_used" INTEGER,
    "receipt_contract_address" TEXT,
    "receipt_root" TEXT,
    "receipt_status" INTEGER,
    "block_timestamp" TIMESTAMP NOT NULL,
    "block_number" INTEGER NOT NULL,
    "block_hash" TEXT NOT NULL,
    "max_fee_per_gas" INTEGER,
    "max_priority_fee_per_gas" INTEGER,
    "transaction_type" INTEGER,
    "receipt_effective_gas_price" INTEGER,

    CONSTRAINT "transactions_pkey" PRIMARY KEY ("hash")
);

-- CreateTable
CREATE TABLE "traces" (
    "transaction_hash" TEXT,
    "transaction_index" INTEGER,
    "from_address" TEXT,
    "to_address" TEXT,
    "value" TEXT,
    "input" TEXT,
    "output" TEXT,
    "trace_type" TEXT NOT NULL,
    "call_type" TEXT,
    "reward_type" TEXT,
    "gas" INTEGER,
    "gas_used" INTEGER,
    "subtraces" INTEGER,
    "trace_address" TEXT,
    "error" TEXT,
    "status" INTEGER,
    "trace_id" TEXT NOT NULL,
    "block_timestamp" TIMESTAMP NOT NULL,
    "block_number" INTEGER NOT NULL,
    "block_hash" TEXT NOT NULL,

    CONSTRAINT "traces_pkey" PRIMARY KEY ("block_number","trace_id")
);

-- CreateTable
CREATE TABLE "exchange_labels" (
    "address" TEXT NOT NULL,
    "exchange" TEXT NOT NULL,
    "label" TEXT,
    "source" TEXT,

    CONSTRAINT "exchange_labels_pkey" PRIMARY KEY ("address")
);

-- CreateIndex
CREATE UNIQUE INDEX "blocks_hash_key" ON "blocks"("hash");

-- CreateIndex
CREATE INDEX "contracts_block_number_idx" ON "contracts"("block_number");

-- CreateIndex
CREATE INDEX "transactions_block_number_idx" ON "transactions"("block_number");

-- CreateIndex
CREATE INDEX "transactions_from_address_idx" ON "transactions"("from_address");

-- CreateIndex
CREATE INDEX "transactions_to_address_idx" ON "transactions"("to_address");

-- CreateIndex
CREATE INDEX "traces_transaction_hash_idx" ON "traces"("transaction_hash");

-- CreateIndex
CREATE INDEX "traces_from_address_idx" ON "traces"("from_address");

-- CreateIndex
CREATE INDEX "traces_to_address_idx" ON "traces"("to_address");

-- CreateIndex
CREATE INDEX "traces_trace_type_idx" ON "traces"("trace_type");

-- CreateIndex
CREATE INDEX "exchange_labels_exchange_idx" ON "exchange_labels"("exchange");
