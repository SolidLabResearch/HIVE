import { ServerResponse, IncomingMessage } from "http";
import { RSPAgentQuery } from "./HTTPServer";
import { QueryReuseRegistry } from "../../reuse/QueryReuseRegistry";


/**
 *
 */
export class POSTHandler {
    private static readonly queryReuseRegistry = new QueryReuseRegistry();

    /**
     *
     * @param request
     * @param response
     * @param body
     * @param rspAgentRecord
     */
    public static async handle(request: IncomingMessage, response: ServerResponse, body: string, rspAgentRecord: Record<string, RSPAgentQuery>) {
        response.setHeader("Content-Type", "application/json");


        let parsedBody: any;

        try {
            parsedBody = JSON.parse(body);


        }

        catch (error) {
            response.statusCode = 400;
            const errorResponse = JSON.stringify({
                error: "Invalid JSON"
            });

            response.setHeader("Content-Length", Buffer.byteLength(errorResponse));
            response.write(errorResponse);
            response.end();
            return;
        }

        try {

            switch (request.url) {
                case "/register": {
                    await this.registerQueryAndAgent(parsedBody, response, rspAgentRecord);
                    break;
                }
            }

        } catch (error) {
            if (!response.headersSent) {
                response.statusCode = 500;
                const errorResponse = JSON.stringify({
                    error: "Internal Server Error",
                    message: error instanceof Error ? error.message : "Unknown Error"
                });

                response.setHeader("Content-Length", Buffer.byteLength(errorResponse));
                response.write(errorResponse);

                response.end();
            }
        }
    }

    /**
     *
     * @param parsedBody
     * @param response
     * @param rspAgentRecord
     */
    private static async registerQueryAndAgent(parsedBody: any, response: ServerResponse, rspAgentRecord: any) {        
        
        if (!parsedBody.id || !parsedBody.rspql_query || !parsedBody.r2s_topic || !parsedBody.data_topic) {
            response.writeHead(400);
            response.end(JSON.stringify({
                error: 'Missing Required Fields'
            }));
            return;
        }

        const decision = await this.queryReuseRegistry.resolveFinalResultRegistration({
            query: parsedBody.rspql_query,
            resultTopic: parsedBody.r2s_topic,
            ownerQueryId: parsedBody.id,
            consumerId: parsedBody.consumer_id || parsedBody.id,
            executionId: parsedBody.execution_id || parsedBody.id,
            approximationConfigHash:
                parsedBody.approximation_config_hash ||
                (parsedBody.approximation_config
                    ? QueryReuseRegistry.buildApproximationConfigHash(parsedBody.approximation_config)
                    : undefined),
        });

        rspAgentRecord[parsedBody.id] = {
            ...parsedBody,
            execution_id: decision.entry.executionId,
            shared_result_topic: decision.entry.resultTopic,
            reuse_decision: decision.decision,
        };

        response.writeHead(200);

        response.end(JSON.stringify({
            message: 'Registered',
            executionId: decision.entry.executionId,
            outputTopic: decision.entry.resultTopic,
            reuseDecision: decision.decision,
        }));
        return;
    }
}
