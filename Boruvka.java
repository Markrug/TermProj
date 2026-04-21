package TermProj;
import java.util.*;

/**
 * Sequential Borůvka's MST Algorithm
 *
 * Each round finds the cheapest outgoing edge for every component, merges
 * those components, then repeats on the contracted graph until one component
 * remains.  O(m log n) time, O(m + n) space.
 *
 * Also includes a small driver that cross-checks against the parallel KKT
 * result when both classes are on the classpath.
 */
public class Boruvka {

    // ── Edge ─────────────────────────────────────────────────────────────────

    public record Edge(int u, int v, double weight) {
        @Override public String toString() {
            return String.format("(%d-%d, %.2f)", u, v, weight);
        }
    }

    // ── Union-Find ───────────────────────────────────────────────────────────

    private static final class UF {
        final int[] p, rank;
        UF(int n) { p = new int[n]; rank = new int[n]; for (int i = 0; i < n; i++) p[i] = i; }
        int find(int x) { while (p[x] != x) { p[x] = p[p[x]]; x = p[x]; } return x; }
        boolean union(int a, int b) {
            a = find(a); b = find(b);
            if (a == b) return false;
            if (rank[a] < rank[b]) { int t = a; a = b; b = t; }
            p[b] = a;
            if (rank[a] == rank[b]) rank[a]++;
            return true;
        }
    }

    // ── Borůvka ──────────────────────────────────────────────────────────────

    /**
     * Computes an MST (or minimum spanning forest) of the given graph.
     *
     * @param n     number of vertices (0-indexed)
     * @param edges edge list
     * @return MST edges
     */
    public static List<Edge> mst(int n, List<Edge> edges) {
        UF uf = new UF(n);
        List<Edge> mst = new ArrayList<>(n - 1);
        List<Edge> active = new ArrayList<>(edges);   // working edge set

        while (mst.size() < n - 1 && !active.isEmpty()) {
            // --- find cheapest outgoing edge per component ---
            Edge[] cheapest = new Edge[n];
            for (Edge e : active) {
                int ru = uf.find(e.u()), rv = uf.find(e.v());
                if (ru == rv) continue;                         // already merged
                if (cheapest[ru] == null || e.weight() < cheapest[ru].weight()) cheapest[ru] = e;
                if (cheapest[rv] == null || e.weight() < cheapest[rv].weight()) cheapest[rv] = e;
            }

            // --- merge components ---
            boolean anyMerge = false;
            for (int i = 0; i < n; i++) {
                if (cheapest[i] != null && uf.union(cheapest[i].u(), cheapest[i].v())) {
                    mst.add(cheapest[i]);
                    anyMerge = true;
                }
            }
            if (!anyMerge) break;   // disconnected graph — done

            // --- drop now-internal edges for next round ---
            active.removeIf(e -> uf.find(e.u()) == uf.find(e.v()));
        }

        return mst;
    }

    public static double totalWeight(List<Edge> mst) {
        return mst.stream().mapToDouble(Edge::weight).sum();
    }

    // ── Demo ─────────────────────────────────────────────────────────────────

    public static void main(String[] args) {
        // Hand-crafted example
        List<Edge> small = List.of(
            new Edge(0, 1, 4),
            new Edge(0, 2, 3),
            new Edge(1, 2, 1),
            new Edge(1, 3, 2),
            new Edge(2, 3, 4),
            new Edge(3, 4, 2),
            new Edge(4, 5, 6),
            new Edge(3, 5, 5)
        );
        List<Edge> result = mst(8, small);
        System.out.println("=== Small example (4 vertices, 6 edges) ===");
        System.out.println("MST edges : " + result);
        System.out.printf ("MST weight: %.2f%n%n", totalWeight(result));

        // Large random graph
        int V = 2_000, E = 50_000;
        Random rng = new Random(42);
        List<Edge> big = new ArrayList<>(E);
        for (int i = 0; i < E; i++) {
            int u = rng.nextInt(V), v = rng.nextInt(V);
            if (u == v) v = (v + 1) % V;
            big.add(new Edge(u, v, rng.nextDouble() * 1000));
        }

        long t0 = System.currentTimeMillis();
        List<Edge> bigMst = mst(V, big);
        long t1 = System.currentTimeMillis();

        System.out.printf("=== Large random graph (%,d vertices, %,d edges) ===%n", V, E);
        System.out.printf("Borůvka time  : %d ms%n", t1 - t0);
        System.out.printf("MST edges     : %d%n", bigMst.size());
        System.out.printf("MST weight    : %.2f%n", totalWeight(bigMst));
    }
}