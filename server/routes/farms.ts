import { Router } from 'express';
import { format, parseISO } from 'date-fns';
import { db } from '@db';
import { eq } from 'drizzle-orm';
import { curtailmentData } from '@db/schema';

const router = Router();

router.get('/api/farms/:date', async (req, res) => {
  try {
    const date = parseISO(req.params.date);
    const formattedDate = format(date, 'yyyy-MM-dd');

    // Fetch curtailment data for the specified date
    const records = await db.query.curtailmentData.findMany({
      where: eq(curtailmentData.date, formattedDate),
      orderBy: (curtailmentData, { desc }) => [desc(curtailmentData.curtailedEnergy)],
    });

    // Calculate total curtailed energy for percentage calculations
    const totalCurtailedEnergy = records.reduce((sum, record) => sum + record.curtailedEnergy, 0);

    // Group records by lead party name
    const groupedData = records.reduce((acc, record) => {
      const group = acc.get(record.leadPartyName) || {
        leadPartyName: record.leadPartyName,
        totalCurtailedEnergy: 0,
        totalPercentageOfTotal: 0,
        totalPayment: 0,
        farms: [],
      };

      const percentageOfTotal = (record.curtailedEnergy / totalCurtailedEnergy) * 100;

      group.totalCurtailedEnergy += record.curtailedEnergy;
      group.totalPayment += record.payment;
      group.farms.push({
        farmId: record.farmId,
        curtailedEnergy: record.curtailedEnergy,
        percentageOfTotal,
        payment: record.payment,
      });

      acc.set(record.leadPartyName, group);
      return acc;
    }, new Map());

    // Calculate total percentages for each group
    const farms = Array.from(groupedData.values()).map(group => ({
      ...group,
      totalPercentageOfTotal: (group.totalCurtailedEnergy / totalCurtailedEnergy) * 100,
    }));

    res.json({ farms });
  } catch (error) {
    console.error('Error fetching farm data:', error);
    res.status(500).json({ error: 'Failed to fetch farm data' });
  }
});

export default router;
