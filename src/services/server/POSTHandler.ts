import { ServerResponse, IncomingMessage } from "http";
import { RSPAgentQuery } from "./HTTPServer";
import { QueryReuseRegistry } from "../../reuse/QueryReuseRegistry";
import { ProductionQueryRegistrationService } from "../reuse/ProductionQueryRegistrationService";


/**
 *
 */
export class POSTHandler {
    private static registrationService = new ProductionQueryRegistrationService(
        new QueryReuseRegistry(),
    );

    public static setRegistrationService(service: ProductionQueryRegistrationService): void {
        this.registrationService = service;
    }

    public static async shutdownRegistrationService(): Promise<void> {
        await this.registrationService.shutdown();
    }

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

        const approach = parsedBody.approach;
        if (approach !== "fetching" && approach !== "approximation" && approach !== "chunked") {
            response.writeHead(400);
            response.end(JSON.stringify({
                error: "Missing or invalid approach",
            }));
            return;
        }

        const registration = await this.registrationService.register({
            approach,
            query: parsedBody.rspql_query,
            requestedOutputTopic: parsedBody.r2s_topic,
            ownerQueryId: parsedBody.id,
            consumerId: parsedBody.consumer_id || parsedBody.id,
            approximationConfigHash:
                parsedBody.approximation_config_hash ||
                (parsedBody.approximation_config
                    ? QueryReuseRegistry.buildApproximationConfigHash(parsedBody.approximation_config)
                    : undefined),
        });

        rspAgentRecord[parsedBody.id] = {
            ...parsedBody,
            execution_id: registration.executionId,
            shared_result_topic: registration.sharedOutputTopic,
            reuse_decision: registration.containmentDecision,
        };

        response.writeHead(200);

        response.end(JSON.stringify({
            message: 'Registered',
            consumerId: registration.consumerId,
            canonicalQueryId: registration.canonicalQueryId,
            executionId: registration.executionId,
            executionCreated: registration.executionCreated,
            reuseHit: registration.reuseHit,
            outputTopic: registration.sharedOutputTopic,
            executionState: registration.executionState,
            reuseDecision: registration.containmentDecision,
            registrationTimestamp: registration.registrationTimestamp,
            producerSnapshots: registration.producerSnapshots,
            workerIds: registration.workerIds,
            producerIdentityMappings: registration.producerIdentityMappings,
            localProducerSpawnCount: registration.localProducerSpawnCount,
            managedProducerMode: registration.managedProducerMode,
        }));
        return;
    }
}
