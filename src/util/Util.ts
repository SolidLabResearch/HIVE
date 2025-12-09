import { Store, Writer } from "n3";
import { ParseOptions } from "rdf-parse/lib/RdfParser";
import { createHash } from "crypto";

const rdfParser = require("rdf-parse").default;
const storeStream = require("rdf-store-stream").storeStream;
const streamifyString = require("streamify-string");
import { QuadContainer } from "../services/operators/s2r";

/**
 * Calculates the least common multiple (LCM) of two numbers.
 * @param {number} a - The first number.
 * @param {number} b - The second number.
 * @returns {number} The least common multiple of a and b.
 */
export function lcm(a: number, b: number): number {
  // Calculate the least common multiple (LCM) of two numbers
  // using the formula: LCM(a, b) = |a * b| / GCD(a, b)
  // where GCD is the greatest common divisor.
  // If either a or b is 0, return 0
  return !a || !b ? 0 : Math.abs(a * b) / gcd(a, b);
}

/**
 * Calculates the greatest common divisor (GCD) of two numbers using Euclidean algorithm.
 * @param {number} a - The first number.
 * @param {number} b - The second number.
 * @returns {number} The greatest common divisor of a and b.
 */
function gcd(a: number, b: number): number {
  return b === 0 ? a : gcd(b, a % b);
}

export type JoinFunction = (
  _a: QuadContainer,
  _b: QuadContainer,
) => QuadContainer;

/**
 * Converts a Turtle format string to an N3 Store.
 * @param {string} text - The Turtle format string to parse.
 * @param {string} baseIRI - Optional base IRI for resolving relative IRIs.
 * @returns {Promise<Store>} A promise that resolves to an N3 Store containing the parsed triples.
 */
export async function turtleStringToStore(
  text: string,
  baseIRI?: string,
): Promise<Store> {
  return await stringToStore(text, { contentType: "text/turtle", baseIRI });
}

/**
 * Converts an N3 Store to a string representation.
 * @param {Store} store - The N3 Store to convert.
 * @returns {string} A string representation of the quads in the store.
 */
export function storeToString(store: Store): string {
  const writer = new Writer();
  return writer.quadsToString(store.getQuads(null, null, null, null));
}

/**
 * Converts a string in a given RDF format to an N3 Store.
 * @param {string} text - The RDF string to parse.
 * @param {ParseOptions} options - Parse options including content type and base IRI.
 * @returns {Promise<Store>} A promise that resolves to an N3 Store containing the parsed triples.
 */
export async function stringToStore(
  text: string,
  options: ParseOptions,
): Promise<Store> {
  const textStream = streamifyString(text);
  const quadStream = rdfParser.parse(textStream, options);
  return await storeStream(quadStream);
}
/**
 * Generates an MD5 hash of the input string after removing all whitespace.
 * @param {string} input_string - The string to hash.
 * @returns {string} The MD5 hash as a hexadecimal string.
 */
export function hash_string_md5(input_string: string) {
  input_string = input_string.replace(/\s/g, "");
  const hash = createHash("md5");
  hash.update(input_string);
  return hash.digest("hex");
}

type _Axis = "x" | "y" | "z";

// /**
//  *
//  * @param order
//  */
// export function generateQuery(order: Axis[] = ['z', 'y', 'x']) {
//     const windowMappings: Record<Axis, {
//         window: string;
//         stream: string;
//         variable: string;
//         property: string;
//         subject: string;
//     }> = {
//         x: {
//             window: ':w1',
//             stream: 'mqtt_broker:accX',
//             variable: '?o',
//             property: 'dahccsensors:x',
//             subject: '?s'
//         },
//         y: {
//             window: ':w2',
//             stream: 'mqtt_broker:accY',
//             variable: '?o2',
//             property: 'dahccsensors:y',
//             subject: '?s2'
//         },
//         z: {
//             window: ':w3',
//             stream: 'mqtt_broker:accZ',
//             variable: '?o3',
//             property: 'dahccsensors:z',
//             subject: '?s3'
//         }
//     };

//     const windowClauses = order.map(axis => {
//         const { window, variable, property, subject } = windowMappings[axis];
//         return `
//     { WINDOW ${window} {
//         ${subject} saref:hasValue ${variable} .
//         ${subject} saref:relatesToProperty ${property} .
//     }}`;
//     }).join(' UNION');

//     const query = `
// PREFIX mqtt_broker: <mqtt://localhost:1883/>
// PREFIX saref: <https://saref.etsi.org/core/>
// PREFIX dahccsensors: <https://dahcc.idlab.ugent.be/Homelab/SensorsAndActuators/>
// PREFIX : <https://rsp.js>

// REGISTER RStream <output> AS
// SELECT ?o ?o2 ?o3
// FROM NAMED WINDOW :w1 ON STREAM mqtt_broker:accX [RANGE 120000 STEP 30000]
// FROM NAMED WINDOW :w2 ON STREAM mqtt_broker:accY [RANGE 120000 STEP 30000]
// FROM NAMED WINDOW :w3 ON STREAM mqtt_broker:accZ [RANGE 120000 STEP 30000]
// WHERE {
//     ${windowClauses}
// }
// `;

//     return query;
// }
