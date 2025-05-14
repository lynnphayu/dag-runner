{
  description = "Development environment for DAG runner";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        database_name = "dag";
        postgres_port = toString 5432;
        postgres_user = "postgres";

        postgres_data = "~/.data/postgres";
        postgres_url =
          "postgresql://${postgres_user}:@localhost:${postgres_port}/${database_name}?sslmode=disable";

      in {
        devShells.default = let
          packages = with pkgs; [ postgresql go ];
          scripts = [
            (pkgs.writeTextFile {
              name = "start-db";
              text = ''
                if lsof -i:${postgres_port} > /dev/null 2>&1; then
                  echo "Port ${postgres_port} is already in use. Please stop the process using this port first."
                  exit 1
                fi

                echo "Creating data directory"
                mkdir -p ${postgres_data}
                chmod 700 ${postgres_data}

                echo "Creating lock directory"
                sudo mkdir -p /run/postgresql
                sudo chown ${postgres_user} /run/postgresql

                echo "Initializing database"
                initdb -D ${postgres_data} --auth=trust --username=${postgres_user}

                echo "Starting database"   
                pg_ctl -D ${postgres_data} -l ${postgres_data}/pg.log -o "-k /tmp -F -p ${postgres_port}" start
                pg_ctl -D ${postgres_data} reload

                echo "Creating database"
                if ! psql -h localhost -p ${postgres_port} -U ${postgres_user} -lqt | cut -d \| -f 1 | grep -qw ${database_name}; then
                  createdb -h localhost -p ${postgres_port} -U ${postgres_user} ${database_name}
                  echo "Database ${database_name} created"
                else
                  echo "Database ${database_name} already exists"
                fi
                export DATABASE_URL=${postgres_url}

                echo "Database started"     
              '';
              executable = true;
              destination = "/bin/start-db";
            })
            (pkgs.writeTextFile {
              name = "stop-db";
              text = ''
                pg_ctl -D ${postgres_data} stop
              '';
              executable = true;
              destination = "/bin/stop-db";
            })
          ];
        in pkgs.mkShell {
          buildInputs = packages ++ scripts;
          shellHook = ''
            export DATABASE_URL=${postgres_url}
          '';

        };
      });
}
