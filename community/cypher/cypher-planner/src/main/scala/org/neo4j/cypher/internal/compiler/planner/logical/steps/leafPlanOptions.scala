/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanFinder
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerConfiguration
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.BestResults
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder

object leafPlanOptions extends LeafPlanFinder {

  override def apply(config: QueryPlannerConfiguration,
                     queryGraph: QueryGraph,
                     interestingOrder: InterestingOrder,
                     context: LogicalPlanningContext): Iterable[BestPlans] = {
    val queryPlannerKit = config.toKit(interestingOrder, context)
    val pickBest = config.pickBestCandidate(context)

    // `candidates` can return the same plan, multiple times, thus we call `distinct` to have to compare less plans in `pickBest`.
    // The reason for not using a Set at this point already, is that the order of `leafPlanners`
    // secretly prefers index seeks over index scans over label scans if they have the same cost.
    // Fixing this appropriately would be more intrusive.
    val leafPlanCandidateLists = config.leafPlanners.candidates(queryGraph, interestingOrder = interestingOrder, context = context).distinct
    val leafPlanCandidateListsWithSelections = queryPlannerKit.select(leafPlanCandidateLists, queryGraph)

    val bestPlansPerAvailableSymbols = leafPlanCandidateListsWithSelections
      .groupBy(_.availableSymbols)
      .values
      .map { bucket =>
      val bestPlan = pickBest(bucket).get

      if (interestingOrder.requiredOrderCandidate.nonEmpty) {
        val sortedLeaves = bucket.flatMap(plan => SortPlanner.planIfAsSortedAsPossible(plan, interestingOrder, context))
        val bestSortedPlan = pickBest(sortedLeaves)
        BestResults(bestPlan, bestSortedPlan)
      } else {
        BestResults(bestPlan, None)
      }
    }

    bestPlansPerAvailableSymbols.map(_.map(context.leafPlanUpdater.apply))
  }
}
