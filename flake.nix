{
  description = "figwal — segmented WAL with forking and reducible watermarks";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      forAll = f: nixpkgs.lib.genAttrs systems (s: f nixpkgs.legacyPackages.${s});
    in
    {
      packages = forAll (pkgs: {
        default = pkgs.buildGoModule {
          pname = "figwal";
          version = "0.5.1";
          src = self;
          vendorHash = null; # stdlib only, no external deps
          subPackages = [ "cmd/figwal" ];
          env.CGO_ENABLED = 0;
        };
      });

      # `nix develop` builds the CLI from the working tree (so it tracks
      # uncommitted changes) and puts `figwal` on PATH. Drive trunks (the
      # figaro-mirroring interface):
      #   figwal xwal init ./demo ir translations chalkboard:map   # -> root trunk t0
      #   figwal xwal send  ./demo t0 '"hello"'
      #   figwal xwal set   ./demo t0 mantra root
      #   figwal xwal set   ./demo t0 system.tags.42 '{"cache":"x"}'
      #   figwal xwal send  ./demo t0 '"more"'
      #   figwal xwal send  ./demo t0:2 '"alt"'   # interior fork -> new trunk t1
      #   figwal xwal trunks ./demo ; figwal xwal dump ./demo t1
      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = [ pkgs.go pkgs.gopls ];
          shellHook = ''
            root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
            bin="$root/.devbin"
            mkdir -p "$bin"
            if ( cd "$root" && go build -o "$bin/figwal" ./cmd/figwal ); then
              export PATH="$bin:$PATH"
              echo "[figwal dev] figwal -> $bin/figwal  (try: figwal xwal --help)"
            else
              echo "[figwal dev] build failed; fix and re-run 'go build -o .devbin/figwal ./cmd/figwal'"
            fi
          '';
        };
      });
    };
}
