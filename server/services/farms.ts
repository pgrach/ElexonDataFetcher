import { db } from "@db";
import { farms, type Farm } from "@db/schema";
import { eq } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";

// Path to BMU mapping file
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

export async function initializeFarms() {
  try {
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);

    // Filter for wind farms and transform data
    const windFarms = bmuMapping
      .filter((bmu: any) => bmu.fuelType === "WIND")
      .map((bmu: any) => ({
        id: bmu.elexonBmUnit,
        name: bmu.bmUnitName,
        fuelType: bmu.fuelType,
        generationCapacity: bmu.generationCapacity,
        leadPartyName: bmu.leadPartyName
      }));

    // Insert farms into database
    for (const farm of windFarms) {
      await db.insert(farms).values(farm).onConflictDoNothing();
    }

    console.log(`Initialized ${windFarms.length} wind farms`);
  } catch (error) {
    console.error('Error initializing farms:', error);
    throw error;
  }
}

export async function getAllFarms(): Promise<Farm[]> {
  return db.select().from(farms).orderBy(farms.name);
}

export async function getFarmById(id: string): Promise<Farm | null> {
  const results = await db
    .select()
    .from(farms)
    .where(eq(farms.id, id))
    .limit(1);
  
  return results[0] || null;
}
