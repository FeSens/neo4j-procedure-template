package example;

import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/**
 * Simple demo on how to use the TraversalAPI.
 * Assumes to be running on the Movie graph.
 */
public class Poc1 {

    static final Label ADDRESS = Label.label("Address");

    @Context
    public Transaction tx;

    @Context
    public Log log;

    /**
     * Uses the Traversal API to return all Person fond by a Depth of 2.
     * This could be much easier with a simple Cypher statement, but serves as a
     * demo onl.
     * 
     * @param walletAddress name of the Person node to start from
     * @return Stream of Person Nodes
     */
    @Procedure(value = "traverse.fluxPruned", mode = Mode.READ)
    @Description("traverses starting from a wallet and returns all transactions pruned by transaction value")
    public Stream<CoActorRecord> findCoActors(@Name("walletAddress") String walletAddress,
            @Name("minContribution") Double minContribution,
            @Name("endNodeLabel") String endNodeLabel) {

        Node address = tx.findNode(ADDRESS, "address", walletAddress);

        final Traverser traverse = tx.traversalDescription()
                .expand(new MinContributionPathExpander(minContribution), InitialBranchState.DOUBLE_ZERO)
                .depthFirst()
                .evaluator(new LabelEvaluator(endNodeLabel))
                .uniqueness(Uniqueness.RELATIONSHIP_PATH)
                .traverse(address);

        return StreamSupport
                .stream(traverse.spliterator(), false)
                .map(Path::endNode)
                .map(CoActorRecord::new);
    }

    /**
     * Expand only paths with relevant amounts
     */
    public static final class MinContributionPathExpander implements PathExpander<Double> {

        private final double minContribution;

        public MinContributionPathExpander(double minContribution) {
            this.minContribution = minContribution;
        }

        @Override
        public Iterable<Relationship> expand(Path path, BranchState<Double> branchState) {
            Iterable<Relationship> relationships = path.endNode().getRelationships(Direction.INCOMING);
            double influx = 0.0;
            double bstate = 1.0;
            for (Relationship relationship : relationships) {
                influx += (double) relationship.getProperty("amount");
            }

            if (path.lastRelationship() == null) {
                bstate = 1.0;
                branchState.setState(bstate);
            } else {
                bstate = branchState.getState();
                double amount = (double) path.lastRelationship().getProperty("amount");
                bstate = bstate * amount / influx;
                branchState.setState(bstate);
            }

            ArrayList<Relationship> filtered = new ArrayList<>();
            for (Relationship r : relationships) {
                double amount = (double) r.getProperty("amount");
                if ((amount / influx) * bstate > this.minContribution) {
                    filtered.add(r);
                }
            }
            return filtered;
        }

        @Override
        public PathExpander reverse() {
            throw new RuntimeException("Not needed for the MonoDirectional Traversal Framework");
        }
    }

    /**
     * See <a href=
     * "https://neo4j.com/docs/java-reference/4.2/javadocs/org/neo4j/procedure/Procedure.html">Procedure</a>
     * <blockquote>
     * A procedure must always return a Stream of Records, or nothing. The record is
     * defined per procedure, as a class
     * with only public, non-final fields. The types, order and names of the fields
     * in this class define the format of the
     * returned records.
     * </blockquote>
     * This is a record that wraps one of the valid return types (in this case a
     * {@link Node}.
     */

    public static final class CoActorRecord {

        public final Node node;

        CoActorRecord(Node node) {
            this.node = node;
        }
    }

    /**
     * Miss-using an evaluator to log out the path being evaluated.
     */
    private final class PathLogger implements Evaluator {

        @Override
        public Evaluation evaluate(Path path) {
            log.info(path.toString());
            return Evaluation.EXCLUDE_AND_CONTINUE;
        }
    }

    private static final class LabelEvaluator implements Evaluator {

        private final Label label;

        private LabelEvaluator(String label) {
            this.label = Label.label(label);
        }

        @Override
        public Evaluation evaluate(Path path) {
            if (path.endNode().hasLabel(label)) {
                return Evaluation.INCLUDE_AND_PRUNE;
            } else {
                return Evaluation.INCLUDE_AND_CONTINUE;
            }
        }
    }
}
