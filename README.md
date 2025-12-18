# Streaming Query Hive

> Combining Multiple Streaming Queries to provide actionable insights. 

## Project Structure

- **📁 src/**: Core source code and implementation
- **📁 experiments/**: Experimental scripts and test scenarios
  - **frequency-comparison/**: Frequency-based experiments
  - **rate-comparison/**: Data rate comparison tests
  - **pattern-analysis/**: Pattern analysis experiments (complex oscillations, exponential patterns)
  - **accuracy-analysis/**: Accuracy analysis experiments
- **📁 analysis/**: Data analysis and processing scripts
  - **accuracy/**: Accuracy analysis tools
  - **comparison/**: Result comparison and extraction tools
  - **visualization/**: Plotting and visualization scripts (Python)
  - **data-generation/**: Test data generation scripts
- **📁 examples/**: Example implementations and demos
- **📁 tests/**: Test scripts and examples
- **📁 docs/**: Documentation and reports
- **📁 logs/**: Experimental results and logs
- **📁 scripts/**: Utility scripts
- **📁 temp-data/**: Temporary data files

## Linting

You run the linter via 
```shell
npm run lint:ts
```

You can automatically fix some issues via
```shell
npm run lint:ts:fix
```

## Example Architecture

The Streaming Query Hive can handle multiple streaming queries from different sources, and utilizes different streaming operators to process the data. The architecture is designed to be modular, allowing for easy integration of new sources and operators. An example architecture combining three different sources and the results from the RDF Stream Processing Agents to solve for a specific Query is shown below:

![Example Architecture](./images/Updated%20Architecture%20-Streaming%20Query%20Hive.png)


In the architecture, it is assumed that the query results from the RSP Agents is being streamed to a MQTT topic. The MQTT topic is then consumed by the Streaming Query Hive, which processes the aggregated results using different streaming operators to solve for the Registered Query. The relationship between the queries to establish that the RSP Agent Queries are part of the Registered Query is established using the Query Containment [[1](#footnote-1)] Relationship.

The Streaming Queries utilized in the architecture are described in the RSP-QL query language [[2](#footnote-2)]. The tool utilized to find if the queries have the Query Containment [[1](#footnote-1)] or the Query Isomorphism [[3](#footnote-3)] is the RSP-QL Containment Checker [[4](#footnote-4)]. The RSP-QL Containment Checker is a tool that checks if a query is contained in another query, and can be used to determine if the results of one query can be used to solve another query. The tool is designed to work with the RSP-QL query language, and builds on the work done by the SPeCS Solver [[5](#footnote-5)] to support aggregation functions and the streaming semantics of the RSP-QL query language.

The MQTT broker can be easily changed with another broker, such as RabbitMQ or Kafka, in the future. 

The resultant query results for the parent query can be streamed to a different MQTT topic, or can be stored in a database for further analysis. Moreover, the results can be reasoned over using a reasoning engine such as EYE-JS[[6](#footnote-6)]. The reasoning engine can be used to infer new knowledge from the query results, and can be used to provide actionable insights from the data.

## License

This code is copyrighted by [Ghent University - imec](https://www.ugent.be/ea/idlab/en) and released under the [MIT Licence](./LICENCE) 

## Contact

For any questions, please contact [Kush](mailto:kushbisen@proton.me) or create an issue in the repository [here](https://github.com/SolidLabResearch/streaming-query-hive/issues). 

### Footnotes

[1]: <a href="https://link.springer.com/referenceworkentry/10.1007/978-0-387-39940-9_1269"> Query Containment </a>

[2]: <a href="https://www.igi-global.com/article/rsp-ql-semantics/129761">RSP-QL Semantics: A Unifying Query Model to Explain Heterogeneity of RDF Stream Processing Systems </a>

[3]: <a href="https://link.springer.com/content/pdf/10.1007/3-540-48005-6_3.pdf"> Matching RDF Graphs </a>

[4]: <a href="https://github.com/SolidLabResearch/rspql-containment-checker"> RSP-QL Containment Checker </a>

[5]: <a href="https://github.com/mirkospasic/SpeCS"> SPeCS Solver </a>

[6]: <a href="https://github.com/eyereasoner/eye-js"> EYE-JS </a>