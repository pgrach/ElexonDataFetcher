import { updateLeadPartyNames } from "../services/curtailment";

async function main() {
  try {
    console.log('Starting lead party name update process...');
    await updateLeadPartyNames();
    console.log('Successfully updated lead party names');
    process.exit(0);
  } catch (error) {
    console.error('Error updating lead party names:', error);
    process.exit(1);
  }
}

main();
