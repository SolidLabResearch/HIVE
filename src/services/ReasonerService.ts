import { n3reasoner } from "eyereasoner/dist";
import { storeToString } from "../util/Util";
const N3 = require('n3');

/**
 * Service to perform reasoning on RDF data using N3 rules.
 */
export class ReasonerService {
    public rules: string;
    /**
     * Creates a new ReasonerService instance.
     * @param {string} rules - The N3 rules to apply during reasoning.
     */
    constructor(rules: string) {
        this.rules = rules;
    }

    /**
     * Gets the current rules.
     * @returns {string} The current N3 rules.
     */
    public get Rules(): string {
        return this.rules;
    }
    /**
     * Sets the current rules.
     * @param {string} value - The new N3 rules to use.
     */
    public set Rules(value: string) {
        this.rules = value;
    }

    /**
     * Performs reasoning on the provided data using the configured rules.
     * @param {string} data - The input data in N3 format.
     * @returns {Promise<string>} A promise resolving to the inferred data in N3 format.
     */
    public async reason(data: string): Promise<string> {
        const n3_parser = new N3.Parser({
            format: 'text/n3',
        });

        const store = new N3.Store();
        const rules = n3_parser.parse(this.rules);
        const triples = n3_parser.parse(data);

        for (const rule of rules) {
            store.addQuad(rule.subject, rule.predicate, rule.object, rule.graph);
        }

        for (const triple of triples) {
            store.addQuad(triple.subject, triple.predicate, triple.object, triple.graph);
        }

        const inferredStore = new N3.Store(
            await n3reasoner(store.getQuads(), undefined, {
                output: 'derivations',
                outputType: 'quads'
            })
        );

        return storeToString(inferredStore)
    }
}