package TermProj;

import TermProj.ParallelKKT.Edge;
import TermProj.ParallelKKT.Graph;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ParallelFilterKruskal {

    static final class UnionFind {
        private final int[] parent, rank;

        UnionFind(int n) {
            parent = new int[n];
            rank = new int[n];
            for (int i = 0; i < n; i++)
                parent[i] = i;
        }

        int find(int i) {
            if (parent[i] != i)
                parent[i] = find(parent[i]);
            return parent[i];
        }

        boolean union(int a, int b) {
            int ra = find(a), rb = find(b);
            if (ra == rb)
                return false;
            if (rank[ra] < rank[rb]) {
                int tmp = ra;
                ra = rb;
                rb = tmp;
            }
            parent[rb] = ra;
            if (rank[ra] == rank[rb])
                rank[ra]++;
            return true;
        }

        boolean connected(int a, int b) {
            return find(a) == find(b);
        }

        UnionFind snapshot() {
            UnionFind copy = new UnionFind(parent.length);
            System.arraycopy(parent, 0, copy.parent, 0, parent.length);
            System.arraycopy(rank, 0, copy.rank, 0, rank.length);
            return copy;
        }
    }

    static final class FilterKruskalTask extends RecursiveTask<List<Edge>> {

        private static final int SEQUENTIAL_THRESHOLD = 1_000;

        private final List<Edge> edges;
        private final int n;
        private final UnionFind uf;

        FilterKruskalTask(List<Edge> edges, int n, UnionFind uf) {
            this.edges = edges;
            this.n = n;
            this.uf = uf;
        }

        @Override
        protected List<Edge> compute() {
            if (edges.isEmpty())
                return Collections.emptyList();

            if (edges.size() <= SEQUENTIAL_THRESHOLD) {
                return sequentialKruskal(edges, uf);
            }

            double pivot = medianOfThree(edges);

            List<Edge> eSmall = new ArrayList<>();
            List<Edge> eLarge = new ArrayList<>();

            for (Edge e : edges) {
                if (e.weight <= pivot)
                    eSmall.add(e);
                else
                    eLarge.add(e);
            }

            if (eSmall.isEmpty() || eLarge.isEmpty()) {
                return sequentialKruskal(edges, uf);
            }

            UnionFind ufSmall = uf.snapshot();
            FilterKruskalTask smallTask = new FilterKruskalTask(eSmall, n, ufSmall);
            smallTask.fork();

            List<Edge> mstSmall = smallTask.join();

            UnionFind ufAfterSmall = uf.snapshot();
            for (Edge e : mstSmall) {
                ufAfterSmall.union(e.u, e.v);
            }

            List<Edge> eLargeFiltered = eLarge.parallelStream()
                    .filter(e -> !ufAfterSmall.connected(e.u, e.v))
                    .collect(Collectors.toList());

            List<Edge> mstLarge = new FilterKruskalTask(
                    eLargeFiltered, n, ufAfterSmall.snapshot()).compute();

            List<Edge> mst = new ArrayList<>(mstSmall.size() + mstLarge.size());
            mst.addAll(mstSmall);
            mst.addAll(mstLarge);
            return mst;
        }

        private static List<Edge> sequentialKruskal(List<Edge> edges, UnionFind uf) {
            List<Edge> sorted = new ArrayList<>(edges);
            sorted.sort(Comparator.comparingDouble(e -> e.weight));

            List<Edge> mst = new ArrayList<>();
            UnionFind local = uf.snapshot();

            for (Edge e : sorted) {
                if (local.union(e.u, e.v))
                    mst.add(e);
            }
            return mst;
        }

        private static double medianOfThree(List<Edge> edges) {
            int mid = edges.size() / 2;

            double a = edges.get(0).weight;
            double b = edges.get(mid).weight;
            double c = edges.get(edges.size() - 1).weight;

            if (a > b) {
                double t = a;
                a = b;
                b = t;
            }
            if (b > c) {
                double t = b;
                b = c;
                c = t;
            }
            if (a > b) {
                double t = a;
                a = b;
                b = t;
            }

            return b;
        }
    }

    public static List<Edge> mst(Graph g, ForkJoinPool pool) {
        if (g.vertices == 0 || g.edges.isEmpty())
            return Collections.emptyList();

        UnionFind uf = new UnionFind(g.vertices);
        FilterKruskalTask task = new FilterKruskalTask(new ArrayList<>(g.edges), g.vertices, uf);

        return pool.invoke(task);
    }

    public static double totalWeight(List<Edge> mst) {
        return mst.stream().mapToDouble(e -> e.weight).sum();
    }

    public static void main(String[] args) {
        System.out.println("Generating graph...");
    }
}