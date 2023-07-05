#pragma once

#include "compressed_column.hpp"
#include "../core/global_definitions.hpp"
#include <list>

namespace CoGaDB {

    /*!
     *  \brief This class represents a delta encoded column with type T.
     */
    template<class T>
    class DeltaEncodedColumn final : public CompressedColumn<T> {

        public:
            /***************** constructors and destructor *****************/
            explicit DeltaEncodedColumn(const std::string &name);

            ~DeltaEncodedColumn() final;

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
                archive(values);// serialize things by passing them to the archive
            }

        private:
            std::vector<T> values;

    };

    /***************** Start of Implementation Section ******************/

    template<class T>
    DeltaEncodedColumn<T>::DeltaEncodedColumn(const std::string &name) : CompressedColumn<T>(name) {
        if(std::is_same<T, bool>::value || std::is_same<T, std::string>::value){
            throw std::invalid_argument("Invalid type: Can't run delta encoding on strings or booleans");
        }
    }

    template<class T>
    DeltaEncodedColumn<T>::~DeltaEncodedColumn() = default;

    template<class T>
    void DeltaEncodedColumn<T>::insert(const ColumnType& col_type) {
        std::cout << "Insert (coltype): " << std::to_string(std::get<T>(col_type)) << std::endl;
        T value = std::get<T>(col_type);
        DeltaEncodedColumn<T>::insert(value);
    }

    template<class T>
    void DeltaEncodedColumn<T>::insert(const T& new_value) {
        // std::cout << "Insert: " << new_value << std::endl;

        if(values.empty()){
            values.emplace_back(new_value);
            // std::cout << "  inserting: " << new_value << std::endl;
        }else{
            T val = values.front();
            for(size_t i = 1; i < values.size(); ++i){
                val += values[i];
            }
            T val_insert = new_value-val;
            values.emplace_back(val_insert);
            // std::cout << "  inserting: " << val_insert << std::endl;
            // for(size_t i = 0; i < values.size(); ++i){
            //     std::cout << values[i] << ",";
            // }
        }



        // if (values.empty()){
        //     values.emplace_front(new_value);
        // }else{
        //     auto itr = values.begin();
        //     T curr_val = values.front();

        //     while(curr_val < new_value && !(itr == values.end())){ 
        //         curr_val += *itr;
        //         ++itr;
        //     }

        //     if(itr == values.begin()){
        //         T diff = *itr-new_value;
        //         *itr=diff; // Replace current reference value with diff
        //         values.emplace_front(new_value);
        //     }
        //     else if(itr == values.end()){
        //         auto last_element = ++values.end();
        //         T diff = new_value - *last_element;
        //         values.emplace_back(diff);
        //     }
        //     else{
        //         T diff_back = curr_val - new_value;
        //         *itr = diff_back;
        //         auto prev = std::prev(itr, 1);
        //         T diff_front = new_value - (curr_val - *prev);
        //         values.insert(itr, diff_front);
        //     }
        // }
    }

    template<typename T>
    template<typename InputIterator>
    void DeltaEncodedColumn<T>::insert(InputIterator first, InputIterator last) {
        for (InputIterator iit = first; iit != last; ++iit){
            insert(iit);                                    
        }
    }

    template<class T>
    ColumnType DeltaEncodedColumn<T>::get(TID tid) {
        auto itr = values.begin();
        T val = values.front();
        for(unsigned int i = 0; i < tid; ++i){
            val += *(++itr);
        }
        return val;
    }

    template<class T>
    std::string DeltaEncodedColumn<T>::print() const noexcept {
        T val = values.front();
        std::string str = !values.empty() ? std::to_string(val) + "\n"  : "";
        for(size_t i = 1; i < values.size(); ++i){
            val += values[i];
            str.append(std::to_string(val) + ": " + std::to_string(values[i]) + "\n");

        }

        return str;

        // return std::accumulate(values.cbegin(), values.cend(), "| " + this->name_ + " |\n________________________\n",
        //                        [](std::string acc, const T &cur) {
        //                            if constexpr(std::is_same_v<std::string, T>)
        //                                return std::move(acc) + "| " + cur + " |\n";
        //                            else
        //                                return std::move(acc) + "| " + std::to_string(cur) + " |\n";
        //                        });
    }

    template<class T>
    size_t DeltaEncodedColumn<T>::size() const noexcept {
        return values.size();
    }

    template<class T>
    std::unique_ptr<ColumnBase> DeltaEncodedColumn<T>::copy() const {
        return std::make_unique<DeltaEncodedColumn<T>>(*this);
    }

    template<class T>
    void DeltaEncodedColumn<T>::update(TID tid, const ColumnType& value) {
        std::cout << "Update element at " << tid << " to " << std::get<T>(value) << std::endl;
        T new_value = std::get<T>(value);
        if(tid == 0){
            if (values.size() > 1){
                values[1] -= new_value;
            }
            values[0] = new_value;
        }else{
            if(tid < (values.size() - 1)){
                T val_after = std::get<T>(get(tid+1));
                T val_after_new = val_after - new_value;
                values[tid+1] = val_after_new;
            }
            values[tid] = new_value - std::get<T>(get(tid-1));
        }
    }

    template<class T>
    void DeltaEncodedColumn<T>::update(PositionList& tids, const ColumnType& value) {
        for(TID tid: tids){
            update(tid, value);
        }   
    }

    template<class T>
    void DeltaEncodedColumn<T>::remove(TID tid) {
        std::cout << "Remove element at " << tid << std::endl;
        if(values.size() == 1) {
            values.clear();
            return;
        }
        if (tid == 0){
            values[1] += values.front();
            values.erase(values.begin());
        }
        else{
            if(tid < (values.size() - 1)){
                T val_before = std::get<T>(get(tid-1));
                T val_after = std::get<T>(get(tid+1));
                T new_val = val_after - val_before;
                values[tid+1] = new_val;
            }
            values.erase(values.begin() + tid);
        }
    }

    template<class T>
    void DeltaEncodedColumn<T>::remove(PositionList& tids) {
        for(TID tid : tids){
            remove(tid);
        }
    }

    template<class T>
    void DeltaEncodedColumn<T>::clearContent() {
        values.clear();
        std::cout << "After clearing content size of values is " << values.size() << std::endl;
    }

    template<class T>
    void DeltaEncodedColumn<T>::store(const std::string & path) {
        std::string path_(path);
         path_ += this->name_;

        std::ofstream outfile(path_.c_str(), std::ofstream::binary | std::ofstream::out | std::ofstream::trunc);
        assert(outfile.is_open());
        cereal::PortableBinaryOutputArchive oarchive(outfile); // Create an output archive
        oarchive(values); 
    }

    template<class T>
    void DeltaEncodedColumn<T>::load(const std::string & path) {
        std::string path_(path);
         path_ += this->name_;

        std::ifstream infile(path_.c_str(), std::ifstream::binary | std::ifstream::in);
        cereal::PortableBinaryInputArchive ia(infile);
        ia(values);
    }


    template<class T>
    T DeltaEncodedColumn<T>::operator[](const int indx) {
        return std::get<T>(get(indx));
    }

    template<class T>
    size_t DeltaEncodedColumn<T>::getSizeInBytes() const noexcept {
        return sizeof(T) * values.size();
    }

    /***************** End of Implementation Section ******************/

}// namespace CoGaDB
