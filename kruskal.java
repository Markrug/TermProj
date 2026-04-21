package TermProj;

import java.util.*;

public class kruskal {

    static final class UnionFind {
        int[] parent, rank;

        UnionFind(int n) {
            parent = new int[n];
            rank = new int[n];
            for (int i = 0; i < n; i++) parent[i] = i;
        }

        int find(int x) {
            if (parent[x] != x)
                parent[x] = find(parent[x]);
            return parent[x];
        }

        boolean union(int a, int b) {
            int ra = find(a), rb = find(b);
            if (ra == rb) return false;
            if (rank[ra] < rank[rb]) {
                int t = ra; ra = rb; rb = t;
            }
            parent[rb] = ra;
            if (rank[ra] == rank[rb]) rank[ra]++;
            return true;
        }
    }

    public static List<ParallelKKT.Edge> mst(ParallelKKT.Graph g) {
        List<ParallelKKT.Edge> edges = new ArrayList<>(g.edges);
        edges.sort(Comparator.comparingDouble(e -> e.weight));

        UnionFind uf = new UnionFind(g.vertices);
        List<ParallelKKT.Edge> mst = new ArrayList<>();

        for (ParallelKKT.Edge e : edges) {
            if (uf.union(e.u, e.v)) {
                mst.add(e);
                if (mst.size() == g.vertices - 1) break;
            }
        }
        return mst;
    }

    public static double totalWeight(List<ParallelKKT.Edge> mst) {
        return mst.stream().mapToDouble(e -> e.weight).sum();
    }
}