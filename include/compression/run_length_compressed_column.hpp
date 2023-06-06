#pragma once

#include "compressed_column.hpp"
#include "core/global_definitions.hpp"

namespace CoGaDB {
    template<class T>
    class RunLengthCompressedColumn final : public CompressedColumn<T> {
    public:
        /***************** constructors and destructor *****************/
        explicit RunLengthCompressedColumn(const std::string &name);

        ~RunLengthCompressedColumn() final;

        void insert(const ColumnType &new_Value) final;

        void insert(const T &new_value) final;

        template<typename InputIterator>
        void insert(InputIterator first, InputIterator last);

        void update(TID tid, const ColumnType &new_value) final;

        void update(PositionList &tid, const ColumnType &new_value) final;

        void remove(TID tid) final;

        // assumes tid list is sorted ascending
        void remove(PositionList &tid) final;

        void clearContent() final;

        ColumnType get(TID tid) final;

        // virtual const std::any* const getRawData()=0;
        std::string print() const noexcept final;

        [[nodiscard]] size_t size() const noexcept final;

        [[nodiscard]] size_t getSizeInBytes() const noexcept final;

        [[nodiscard]] virtual std::unique_ptr<ColumnBase> copy() const;

        void store(const std::string &path) final;
        void load(const std::string &path) final;

        T operator[](int index) final;

        /**
         * @brief Serialization method called by Cereal. Implement this method in your compressed columns to get serialization working.
         */
        template<class Archive>
        void serialize(Archive &archive) {
            //TODO: implement
            archive();// serialize things by passing them to the archive
        }

    private:
        int cntElements = 0;
        std::vector<std::tuple<unsigned int, T>> values;
    };

    /***************** Start of Implementation Section ******************/

    template<class T>
    RunLengthCompressedColumn<T>::RunLengthCompressedColumn(const std::string &name) : CompressedColumn<T>(name) {
        //TODO: implement
    }

    template<class T>
    RunLengthCompressedColumn<T>::~RunLengthCompressedColumn() = default;

    template<class T>
    void RunLengthCompressedColumn<T>::insert(const ColumnType &new_Value) {
        //TODO: implement
        if (new_Value.empty()) return;
        if (typeid(T) == new_Value.type()) {
            this->insert(new_Value);
            return;
        }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::insert(const T &new_value) {
        //TODO: implement
        if(values.size() == 0) {
            values.push_back(std::make_tuple(1, new_value));
        } else if (std::get<1>(values.back()) == new_value) {
            std::get<0>(values.back())++;
        } else values.push_back(std::make_tuple(1, new_value));
        cntElements++;
        return;
    }

    template<typename T>
    template<typename InputIterator>
    void RunLengthCompressedColumn<T>::insert(InputIterator, InputIterator) {
        //TODO: implement
        for (InputIterator it = first; it != last; ++it) { 
            this->insert(it);
        }
        return;
    }

    template<class T>
    ColumnType RunLengthCompressedColumn<T>::get(TID tid) {
        //TODO: implement
        if (cntElements > tid) {
            int sum = 0;
            for (int i = 0; i < values.size(); i++) {
                sum += std:get<0>(values[i]);

                if (tid < sum) {
                    return std::get<1>(values[i]);
                }
            }
        }
        return {};
    }

    template<class T>
    std::string RunLengthCompressedColumn<T>::print() const noexcept {
        //TODO: implement
        for (int i = 0; i < values.size(); i++) {
            std::cout << "Häufigkeit: " << std::get<0>(values[i]) << " Wert: " << std::get<1>(values[i]) << std::endl;
        }
        return {};
    }

    template<class T>
    size_t RunLengthCompressedColumn<T>::size() const noexcept {
        //TODO: implement
        return cntElements;
    }

    template<class T>
    std::unique_ptr<ColumnBase> RunLengthCompressedColumn<T>::copy() const {
        //TODO: implement
        return {};
    }

    template<class T>
    void RunLengthCompressedColumn<T>::update(TID tid, const ColumnType &new_value) {
        //TODO: implement
        if (new_value.empty() || cntElements <= tid) {
            return;
        } else {
            int sum = 0;
            for (int i = 0; i < values.size(); i++) {
                sum += std::get<0>(values[i]);
                if (tid < sum) {
                    if (std::get<1>(values[i]) == new_value) {
                        return;
                    }
                    if (std::get<0>(values[i]) == 1) {
                        std::get<1>(values[i]) = new_value;
                        return;
                    }

                    std::vector<std::tuple<int, T>> new_values;

                    for (int j = 0; j < cntElements; j++) {
                        T value = this->get(j);

                        if (tid == j) {
                            value = new_value;
                        }

                        if (new_values.size() == 0) {
                            new_values.push_back(std::make_tuple(1, value));
                        } else if (std::get<1>(new_values.back()) == value) {
                            std::get<0>(new_values.back())++;
                        } else {
                            new_values.push_back(std::make_tuple(1, value));
                        }
                    }
                    values = new_values;
                    return;
                }
            }
        }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::update(PositionList &tid, const ColumnType &new_value) {
        //TODO: implement
        for (int i = 0; i < tid->size(); i++) {
            this->update((*tid)[i], new_value);
        }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::remove(TID tid) {
        //TODO: implement
        int sum = 0;
        for (int i = 0; i < values.size(); i++) {
            sum += std::get<0>(values[i]);
            if (tid < sum) {
                if (std::get<0>(values[i]) > 1) {
                    std::get<0>(values[i])--;
                    cntElements--;
                    return;
                }
                if (std::get<1>(values[i - 1]) == std::get<1>(values[i + 1])) {
                    std::get<0>(values[i - 1]) += std::get<0>(values[i + 1]);
                    values.erase(values.begin() + i + 1);
                    values.erase(values.begin() + i);
                    cntElements--;
                    return;
                }
                values.erase(values.begin() + i);
                cntElements--;
                return;
            }
        }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::remove(PositionList &tid) {
        //TODO: implement
        for (int i = 0; i < tid->size(); i++) {
            this->remove((tid)[i]);
        }
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::clearContent() {
        //TODO: implement
        values.clear();
        cntElements = 0;
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::store(const std::string &path) {
        //TODO: implement
        std::string path_(path);
        path_ += "/";
        path_ += this->name;

        std::ofstream outfile (path_.c_str(),std::ios_base::binary | std::ios_base::out);
        boost::archive::binary_oarchive oa(outfile);

        oa << values;

        outfile.flush();
        outfile.close();
        return;
    }

    template<class T>
    void RunLengthCompressedColumn<T>::load(const std::string &path) {
        //TODO: implement
        std::string path_(path);
        path_ += "/";
        path_ += this->name;

        std::ifstream infile (path_.c_str(),std::ios_base::binary | std::ios_base::in);
        boost::archive::binary_iarchive ia(infile);
        ia >> values;
        infile.close();

        cntElements = 0;
        for(int i = 0; i < values.size();i++){
            cntElements += std::get<0>(values[i]);
        }
        return;
    }


    template<class T>
    T RunLengthCompressedColumn<T>::operator[](const int index) {
        //TODO: implement
        static T val;
        if (cntElements > index) {
            int sum = 0;
            for (int i = 0; i < values.size(); i++) {
                sum += std::get<0>(values[i]);

                if (index < sum) {
                    val = std::get<1>(values[i]);
                    return val;
                }
            }
        }
        return val;
    }

    template<class T>
    size_t RunLengthCompressedColumn<T>::getSizeInBytes() const noexcept {
        //TODO: implement
        return values.size() * (sizeof(T) + sizeof(int));
    }

    /***************** End of Implementation Section ******************/

}// namespace CoGaDB
