package TermProj;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * Parallel Karger–Klein–Tarjan (KKT) Randomized MST Algorithm
 *
 * Complexity: O(m) expected work, O(log²n) time
 */
public class ParallelKKT {

    public static final class Edge {
        public final int u, v;
        public final double weight;

        public Edge(int u, int v, double weight) {
            this.u = u;
            this.v = v;
            this.weight = weight;
        }

        @Override
        public String toString() {
            return String.format("(%d-%d, %.2f)", u, v, weight);
        }
    }

    public static final class Graph {
        public final int vertices; // number of vertices (0-indexed)
        public final List<Edge> edges;

        public Graph(int vertices, List<Edge> edges) {
            this.vertices = vertices;
            this.edges = Collections.unmodifiableList(edges);
        }

        public boolean isEmpty() {
            return vertices <= 1 || edges.isEmpty();
        }

        public boolean isFullyContracted() {
            return vertices <= 1;
        }
    }

    private static final class UnionFind {
        private final int[] parent, rank;

        UnionFind(int n) {
            parent = new int[n];
            rank = new int[n];
            for (int i = 0; i < n; i++)
                parent[i] = i;
        }

        int find(int x) {
            while (parent[x] != x) {
                parent[x] = parent[parent[x]]; // path halving
                x = parent[x];
            }
            return x;
        }

        // Returns true if the two nodes were in different components.
        boolean union(int a, int b) {
            int ra = find(a), rb = find(b);
            if (ra == rb)
                return false;
            if (rank[ra] < rank[rb]) {
                int t = ra;
                ra = rb;
                rb = t;
            }
            parent[rb] = ra;
            if (rank[ra] == rank[rb])
                rank[ra]++;
            return true;
        }

        boolean connected(int a, int b) {
            return find(a) == find(b);
        }
    }

    private static ContractResult boruvka(Graph g, int rounds) {
        List<Edge> mstEdges = new ArrayList<>();

        for (int round = 0; round < rounds; round++) {
            if (g.edges.isEmpty() || g.vertices <= 1)
                break;

            int n = g.vertices; // current (contracted) vertex count
            UnionFind uf = new UnionFind(n); // fresh UF each round

            Edge[] cheapest = new Edge[n];
            for (Edge e : g.edges) {
                int ru = uf.find(e.u), rv = uf.find(e.v);
                if (ru == rv)
                    continue;
                if (cheapest[ru] == null || e.weight < cheapest[ru].weight)
                    cheapest[ru] = e;
                if (cheapest[rv] == null || e.weight < cheapest[rv].weight)
                    cheapest[rv] = e;
            }

            boolean anyMerge = false;
            for (int i = 0; i < n; i++) {
                if (cheapest[i] != null && uf.union(cheapest[i].u, cheapest[i].v)) {
                    mstEdges.add(cheapest[i]);
                    anyMerge = true;
                }
            }
            if (!anyMerge)
                break;

            int[] label = new int[n];
            Arrays.fill(label, -1);
            int[] compId = new int[n];
            int numComp = 0;
            for (int i = 0; i < n; i++) {
                int r = uf.find(i);
                if (label[r] == -1)
                    label[r] = numComp++;
                compId[i] = label[r];
            }

            List<Edge> contracted = new ArrayList<>();
            for (Edge e : g.edges) {
                int cu = compId[e.u], cv = compId[e.v];
                if (cu != cv)
                    contracted.add(new Edge(cu, cv, e.weight));
            }
            g = new Graph(numComp, contracted);
        }

        return new ContractResult(mstEdges, g);
    }

    private record ContractResult(List<Edge> mstEdges, Graph contracted) {
    }

    private static List<Edge> removeFHeavy(int n, List<Edge> forestEdges, List<Edge> candidates) {
        
        @SuppressWarnings("unchecked")
        List<double[]>[] tree = new List[n]; // tree[u] = list of {neighbor, edgeWeight}
        for (int i = 0; i < n; i++)
            tree[i] = new ArrayList<>();
        for (Edge e : forestEdges) {
            tree[e.u].add(new double[] { e.v, e.weight });
            tree[e.v].add(new double[] { e.u, e.weight });
        }

        int[] parent = new int[n];
        double[] parentW = new double[n];
        int[] depth = new int[n];
        int[] compRoot = new int[n];
        Arrays.fill(parent, -1);
        Arrays.fill(compRoot, -1);

        Queue<Integer> queue = new ArrayDeque<>();
        for (int start = 0; start < n; start++) {
            if (compRoot[start] != -1)
                continue;
            compRoot[start] = start;
            depth[start] = 0;
            queue.add(start);
            while (!queue.isEmpty()) {
                int u = queue.poll();
                for (double[] nb : tree[u]) {
                    int v = (int) nb[0];
                    double w = nb[1];
                    if (compRoot[v] != -1)
                        continue; // already visited
                    compRoot[v] = compRoot[start];
                    parent[v] = u;
                    parentW[v] = w;
                    depth[v] = depth[u] + 1;
                    queue.add(v);
                }
            }
        }

        return candidates.parallelStream()
                .filter(e -> {
                    if (compRoot[e.u] != compRoot[e.v])
                        return true; // no F-path - keep
                    return e.weight <= pathBottleneck(e.u, e.v, depth, parent, parentW);
                })
                .collect(Collectors.toList());
    }

    private static double pathBottleneck(int u, int v,
            int[] depth, int[] parent, double[] parentW) {
        double maxW = 0;
        // Bring u and v to the same depth
        while (depth[u] > depth[v]) {
            maxW = Math.max(maxW, parentW[u]);
            u = parent[u];
        }
        while (depth[v] > depth[u]) {
            maxW = Math.max(maxW, parentW[v]);
            v = parent[v];
        }
        
        while (u != v) {
            maxW = Math.max(maxW, Math.max(parentW[u], parentW[v]));
            u = parent[u];
            v = parent[v];
        }
        return maxW;
    }

    private static Graph randomSubsample(Graph g, double p, Random rng) {
        List<Edge> sampled = new ArrayList<>((int) (g.edges.size() * p * 1.5));
        for (Edge e : g.edges) {
            if (rng.nextDouble() < p)
                sampled.add(e);
        }
        return new Graph(g.vertices, sampled);
    }

    private static final int BORUVKA_ROUNDS = 3; // contract before recursing
    private static final int BASE_EDGE_THRESHOLD = 256;

    private static class KKTTask extends RecursiveTask<List<Edge>> {
        private final Graph g;
        private final Random rng;

        KKTTask(Graph g, Random rng) {
            this.g = g;
            this.rng = rng;
        }

        @Override
        protected List<Edge> compute() {

            if (g.vertices <= 1 || g.edges.isEmpty())
                return Collections.emptyList();
            if (g.edges.size() <= BASE_EDGE_THRESHOLD) {
                return kruskal(g);
            }

            // ─ Step 1: contract with Borůvka
            ContractResult br = boruvka(g, BORUVKA_ROUNDS);
            Graph contracted = br.contracted();
            List<Edge> mst = new ArrayList<>(br.mstEdges());

            if (contracted.isFullyContracted() || contracted.edges.isEmpty())
                return mst;

            // Step 2: sub-sample edges
            Graph H = randomSubsample(contracted, 0.5, rng);

            // Step 3: recursively find MST of H
            KKTTask task3 = new KKTTask(H, new Random(rng.nextLong()));
            task3.fork();
            List<Edge> forestF = task3.join();

            // ─Step 4: remove F-heavy edges
            List<Edge> light = removeFHeavy(contracted.vertices, forestF, contracted.edges);
            Graph reduced = new Graph(contracted.vertices, light);

            // Step 5: recursively find MST of reduced graph (tail call) ─
            KKTTask task5 = new KKTTask(reduced, new Random(rng.nextLong()));
            List<Edge> mst2 = task5.compute();

            mst.addAll(mst2);
            return mst;
        }
    }

    private static List<Edge> kruskal(Graph g) {
        List<Edge> sorted = new ArrayList<>(g.edges);
        sorted.sort(Comparator.comparingDouble(e -> e.weight));
        UnionFind uf = new UnionFind(g.vertices);
        List<Edge> mst = new ArrayList<>();
        for (Edge e : sorted) {
            if (uf.union(e.u, e.v)) {
                mst.add(e);
                if (mst.size() == g.vertices - 1)
                    break;
            }
        }
        return mst;
    }

    public static List<Edge> mst(Graph g, ForkJoinPool pool) {
        KKTTask task = new KKTTask(g, new Random());
        return pool.invoke(task);
    }

    public static double totalWeight(List<Edge> mst) {
        return mst.stream().mapToDouble(e -> e.weight).sum();
    }

}
