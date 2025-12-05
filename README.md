# Streaming Query Hive

Combining Multiple Streaming Queries to provide actionable insights.

## Quick Start

### Installation
```bash
npm install
npm run build
```

### Run Experiments
```bash
# Setup experiment infrastructure
npm run experiment:setup

# Run quick test
npm run experiment:quick-test

# Run full experiments
npm run experiment:run

# Analyze results
npm run experiment:analyze
```

## Documentation

- **[Setup Guide](docs/SETUP_GUIDE.md)** - Configuration and getting started
- **[Architecture](docs/ARCHITECTURE.md)** - System design and patterns
- **[Approach Orchestrators](docs/APPROACH_ORCHESTRATORS.md)** - Detailed approach documentation
- **[Approach Comparison](docs/APPROACH_COMPARISON.md)** - Performance and use cases
- **[Performance Analysis](docs/PERFORMANCE_ANALYSIS.md)** - Benchmarks and optimization

## Approaches

The project implements three streaming query processing approaches:

1. **Fetching Client Side** (`fetching-client-side`) - Ground truth baseline with centralized processing
2. **Chunked Query Approach** (`chunked-query-approach`) - Scalable distributed chunk-based aggregation
3. **Approximation Approach** (`approximation-approach`) - Fast approximate processing

See [APPROACH_COMPARISON.md](docs/APPROACH_COMPARISON.md) for detailed comparison.

## Development

### Linting

Run the linter:
```bash
npm run lint:ts
```

Auto-fix issues:
```bash
npm run lint:ts:fix
```

### Testing

Verify all approaches:
```bash
npx ts-node scripts/test-all-approaches.ts
```

## Architecture

The Streaming Query Hive can handle multiple streaming queries from different sources, and utilizes different streaming operators to process the data. The architecture is designed to be modular, allowing for easy integration of new sources and operators. An example architecture combining three different sources and the results from the RDF Stream Processing Agents to solve for a specific Query is shown below:

![Example Architecture](./images/Updated%20Architecture%20-Streaming%20Query%20Hive.png)


In the architecture, it is assumed that the query results from the RSP Agents is being streamed to a MQTT topic. The MQTT topic is then consumed by the Streaming Query Hive, which processes the aggregated results using different streaming operators to solve for the Registered Query. The relationship between the queries to establish that the RSP Agent Queries are part of the Registered Query is established using the Query Containment [[1](#footnote-1)] Relationship.

The Streaming Queries utilized in the architecture are described in the RSP-QL query language [[2](#footnote-2)]. The tool utilized to find if the queries have the Query Containment [[1](#footnote-1)] or the Query Isomorphism [[3](#footnote-3)] is the RSP-QL Containment Checker [[4](#footnote-4)]. The RSP-QL Containment Checker is a tool that checks if a query is contained in another query, and can be used to determine if the results of one query can be used to solve another query. The tool is designed to work with the RSP-QL query language, and builds on the work done by the SPeCS Solver [[5](#footnote-5)] to support aggregation functions and the streaming semantics of the RSP-QL query language.

The MQTT broker can be easily changed with another broker, such as RabbitMQ or Kafka, in the future. 

The resultant query results for the parent query can be streamed to a different MQTT topic, or can be stored in a database for further analysis. Moreover, the results can be reasoned over using a reasoning engine such as EYE-JS[[6](#footnote-6)]. The reasoning engine can be used to infer new knowledge from the query results, and can be used to provide actionable insights from the data.

## License

This code is copyrighted by [Ghent University - imec](https://www.ugent.be/ea/idlab/en) and released under the [MIT Licence](./LICENCE) 

## Project Structure

```
streaming-query-hive/
├── src/
│   ├── approaches/          # Approach orchestrators
│   ├── orchestrator/        # Core orchestration logic
│   ├── services/            # Worker factory and services
│   ├── operators/           # Stream processing operators
│   └── config/              # Configuration and mappings
├── scripts/
│   ├── benchmarks/          # Experiment scripts
│   ├── setup/               # Setup scripts
│   └── analysis/            # Analysis tools
├── docs/                    # Documentation
└── results/                 # Experiment results
```

## License

This code is copyrighted by [Ghent University - imec](https://www.ugent.be/ea/idlab/en) and released under the [MIT License](./LICENCE.md)

## Contact

For questions, please contact [Kush](mailto:kushbisen@proton.me) or create an issue [here](https://github.com/SolidLabResearch/streaming-query-hive/issues).

## References

1. [Query Containment](https://link.springer.com/referenceworkentry/10.1007/978-0-387-39940-9_1269)
2. [RSP-QL Semantics: A Unifying Query Model](https://www.igi-global.com/article/rsp-ql-semantics/129761)
3. [Matching RDF Graphs](https://link.springer.com/content/pdf/10.1007/3-540-48005-6_3.pdf)
4. [RSP-QL Containment Checker](https://github.com/SolidLabResearch/rspql-containment-checker)
5. [SPeCS Solver](https://github.com/mirkospasic/SpeCS)
6. [EYE-JS](https://github.com/eyereasoner/eye-js)