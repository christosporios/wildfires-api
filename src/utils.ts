import { Wildfire } from "./types";
import fs from 'fs/promises';
import path from 'path';

// Function to validate Wildfire object
export function isValidWildfire(obj: any): obj is Wildfire {
    return (
        typeof obj === 'object' &&
        typeof obj.id === 'string' &&
        typeof obj.name === 'string' &&
        Array.isArray(obj.boundingBox) &&
        obj.boundingBox.length === 2 &&
        Array.isArray(obj.boundingBox[0]) &&
        obj.boundingBox[0].length === 2 &&
        Array.isArray(obj.boundingBox[1]) &&
        obj.boundingBox[1].length === 2 &&
        Array.isArray(obj.position) &&
        obj.position.length === 2 &&
        typeof obj.zoom === 'number' &&
        typeof obj.start === 'string' &&
        (obj.end === undefined || typeof obj.end === 'string') &&
        typeof obj.timezone === 'string' &&
        typeof obj.metarAirport === 'string'
    );
}

export async function loadWildfireConfigs(): Promise<Map<string, Wildfire>> {
    const wildfireConfigsDir = path.join('.', 'wildfire-configs');
    const wildfireConfigs = new Map<string, Wildfire>();

    try {
        const files = await fs.readdir(wildfireConfigsDir);

        for (const file of files) {
            if (file.endsWith('.json')) {
                const filePath = path.join(wildfireConfigsDir, file);
                try {
                    const fileContent = await fs.readFile(filePath, 'utf-8');
                    const config = JSON.parse(fileContent);

                    if (isValidWildfire(config)) {
                        wildfireConfigs.set(config.id, config);
                    } else {
                        console.error(`Invalid wildfire config in file: ${file}`);
                    }
                } catch (error) {
                    console.error(`Error reading or parsing file ${file}: ${error}`);
                }
            }
        }

        console.log(`Loaded ${wildfireConfigs.size} valid wildfire configs: ${Array.from(wildfireConfigs.keys()).join(', ')}`);
        return wildfireConfigs;
    } catch (error) {
        console.error(`Error reading wildfire configs directory: ${error}`);
        return new Map();
    }
}

